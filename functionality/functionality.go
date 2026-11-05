package functionality

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/segmentio/kafka-go"
	"twitter_clone/connections"
	pb "twitter_clone/tweets"

)


var CelebThreshold = 1000


func SignupBatch(users, emails []string) bool {
	//if 2+ nodes are down, insert will timeout as it should.
	c, cancel := context.WithTimeout(connections.Ctx, 2*time.Second)
	defer cancel()

	var values []any
	for i := range users {
		values = append(values, users[i], emails[i])
	}
	query := "INSERT INTO users (username, email) VALUES" +
			 strings.TrimSuffix(strings.Repeat(" (?, ?),", len(users)), ",") +
			 " ON DUPLICATE KEY UPDATE username = username"

	_, err := connections.Mysql.ExecContext(c, strings.TrimSuffix(query, ","), values...)
	return err == nil
}

//just to see if its faster than leader-based
func SignupLeaderless(users, emails []string) bool {
	//blind write to all 3, no check, like a Cassandra INSERT
	var kvs []*pb.KV
	for i := range users {
		kvs = append(kvs, &pb.KV{Key: []byte("email:" + emails[i]), Value: []byte(users[i])})
	}
	done := make(chan bool, 3)
	for _, db := range connections.Tweets {
		go func(db pb.TweetsClient) {
			db.PutBatch(connections.Ctx, &pb.PutBatchRequest{Kvs: kvs})
			done <- true
		}(db)
	}
	<-done
	return true
}

func FollowBatch(users, targets []string) {
	insert := "INSERT INTO follows (user, target) VALUES" + strings.Repeat(" (?, ?),", len(users))
	update := "UPDATE users SET follower_count = follower_count + 1 WHERE username IN (" + strings.Repeat("?, ", len(targets))
	var pairs, followed []any
	for i := range users {
		pairs = append(pairs, users[i], targets[i])
		followed = append(followed, targets[i])
	}
	tx, _ := connections.Mysql.Begin()
	if _, err := tx.Exec(strings.TrimSuffix(insert, ","), pairs...); err != nil {
		tx.Rollback()
		return
	}
	tx.Exec(strings.TrimSuffix(update, ", ")+")", followed...)
	tx.Commit()
}

func Tweet(user, tweet string) {
	now := time.Now()

	if !UserExists(user) {
		return
	}

	// build a batch to write to LSM (outbox) containing the actual tweet entry and the mail
	key := user + ":" + fmt.Sprintf("%020d", now.UnixNano())
	kvs := []*pb.KV{{
		Key:   []byte(key),
		Value: []byte(tweet),
	}}
	if !IsCeleb(user) {
		mail, _ := json.Marshal(connections.Mail{User: user, Tweet: tweet, Time: now.UnixNano()})
		kvs = append(kvs, &pb.KV{Key: []byte("outbox:" + key), Value: mail})
	}

	results := make(chan error, 3)
	for _, db := range connections.Tweets {
		// write to all 3 dbs at once
		go func(db pb.TweetsClient) {
			_, err := db.PutBatch(connections.Ctx, &pb.PutBatchRequest{Kvs: kvs})
			results <- err
		}(db)
	}
	for i := 0; i < 3; i++ {
		if <-results == nil {
			break
		}
	}
}

// sending outbox to kafka
func SendOutbox(db *pebble.DB) {
	for {
		iter, _ := db.NewIter(&pebble.IterOptions{LowerBound: []byte("outbox:"), UpperBound: []byte("outbox;")})
		for iter.First(); iter.Valid(); iter.Next() {
			var mail connections.Mail
			json.Unmarshal(iter.Value(), &mail)
			
			err := connections.Producer.WriteMessages(connections.Ctx, kafka.Message{
				Key:   []byte(mail.User),
				Value: slices.Clone(iter.Value()),
			})
			if err == nil {
				db.Delete(slices.Clone(iter.Key()), pebble.Sync)
			}
		}
		iter.Close()
		time.Sleep(time.Second)
	}
}

func Repair() {
	//using an in memory map to save repeated reads to the disk
	all := map[string][]byte{}

	var inMemory [3]map[string]bool
	for i := range connections.Tweets {
		inMemory[i] = map[string]bool{}
		//outbox keys all sit between "outbox:" and "outbox;", so read everything before and after that block
		for _, part := range []*pb.ScanRequest{{Upper: []byte("outbox:")}, {Lower: []byte("outbox;")}} {
			stream, err := connections.Tweets[i].Scan(connections.Ctx, part)
			if err != nil {
				continue
			}
			for {
				kv, err := stream.Recv()
				if err != nil {
					break
				}
				all[string(kv.Key)] = kv.Value
				inMemory[i][string(kv.Key)] = true
			}
		}
	}
	//for each of the 3 in memory copies
	for i := range connections.Tweets {
		//batch whatever is missing from the master map
		var missing []*pb.KV
		for key, value := range all {
			if !inMemory[i][key] {
				missing = append(missing, &pb.KV{Key: []byte(key), Value: value})
			}
		}
		if len(missing) > 0 {
			connections.Tweets[i].PutBatch(connections.Ctx, &pb.PutBatchRequest{Kvs: missing})
		}
	}
}

//getters
func UserExists(user string) bool {
	var n int
	connections.Mysql.QueryRow("SELECT COUNT(*) FROM users WHERE username = ?", user).Scan(&n)
	return n > 0
}

func GetFollowerCount(user string) int {
	var count int
	connections.Mysql.QueryRow("SELECT follower_count FROM users WHERE username = ?", user).Scan(&count)
	return count
}

func IsCeleb(user string) bool {
	return GetFollowerCount(user) >= CelebThreshold
}

func GetFollowers(user string) []string {
	var followers []string
	rows, _ := connections.Mysql.Query("SELECT user FROM follows WHERE target = ?", user)
	for rows.Next() {
		var follower string
		rows.Scan(&follower)
		followers = append(followers, follower)
	}
	rows.Close()
	return followers
}

func GetProfile(target string) []string {
	//naw screw 3 pointer merge sort. we just put everything into a map from all 3 db and sort
	all := map[string]string{}
	for _, db := range connections.Tweets {
		stream, err := db.Scan(connections.Ctx, &pb.ScanRequest{
			Lower: []byte(target + ":"),
			Upper: []byte(target + ";"),
		})
		if err != nil {
			continue
		}
		for {
			kv, err := stream.Recv()
			if err != nil {
				break
			}
			all[string(kv.Key[len(target+":"):])+":"+target] = string(kv.Value)
		}
	}

	var profile []string
	for _, k := range slices.Backward(slices.Sorted(maps.Keys(all))) {
		nanos, _ := strconv.ParseInt(k[:20], 10, 64)
		profile = append(profile, time.Unix(0, nanos).Format("15:04")+" "+k[21:]+": "+all[k])
	}
	
	return profile
}

func OpenMail(user string) []string {
	if !UserExists(user) {
		return nil
	}

	//keyed by time and user
	//map automatically deduplicates non-idempotent mail
	all := map[string]string{}
	texts, redisErr := connections.Redis.LRange(connections.Ctx, "mailbox:"+user, 0, -1).Result()
	for _, text := range texts {
		var mail connections.Mail
		json.Unmarshal([]byte(text), &mail)
		all[fmt.Sprintf("%020d:%s", mail.Time, mail.User)] = mail.Tweet
	}

	var pull []string
	rows, _ := connections.Mysql.Query("SELECT f.target, u.follower_count FROM follows f JOIN users u ON u.username = f.target WHERE f.user = ?", user)
	for rows.Next() {
		var followee string
		var count int
		rows.Scan(&followee, &count)
		//collect whoever is a celeb or all if redis is down
		if redisErr != nil || count >= CelebThreshold {
			pull = append(pull, followee)
		}
	}
	rows.Close()

	for _, followee := range pull {
		var newest connections.Mail
		hasTweeted := false
		//if one db is still recovery we don't know which is latest
		//check all 3 dbs to determine latest
		for _, db := range connections.Tweets {
			reply, err := db.Last(connections.Ctx, &pb.ScanRequest{
				Lower: []byte(followee + ":"),
				Upper: []byte(followee + ";"),
			})
			if err == nil && reply.Found {
				nanos, _ := strconv.ParseInt(string(reply.Kv.Key[len(followee+":"):]), 10, 64)
				if !hasTweeted || nanos > newest.Time {
					newest = connections.Mail{User: followee, Tweet: string(reply.Kv.Value), Time: nanos}
					hasTweeted = true
				}
			}
		}
		if hasTweeted {
			all[fmt.Sprintf("%020d:%s", newest.Time, newest.User)] = newest.Tweet
		}
	}

	var feed []string
	for _, k := range slices.Backward(slices.Sorted(maps.Keys(all))) {
		if len(feed) == 20 {
			break
		}
		nanos, _ := strconv.ParseInt(k[:20], 10, 64)
		feed = append(feed, time.Unix(0, nanos).Format("15:04")+" "+k[21:]+": "+all[k])
	}
	return feed
}