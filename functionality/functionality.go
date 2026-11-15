package functionality

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/segmentio/kafka-go"
	"twitter_clone/connections"
)


var CelebThreshold = 1000


func SignupBatch(users, emails []string) bool {
	//if 2+ nodes are down (no response in 2 seconds), insert will timeout as it should.
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
	now := time.Now().UnixNano()

	if !UserExists(user) {
		return
	}

	// save the tweet and its outbox mail in one batch: either both land or neither does
	batch := connections.Cassandra.NewBatch(gocql.LoggedBatch)
	batch.Query("INSERT INTO posts (username, posted_at, tweet) VALUES (?, ?, ?)", user, now, tweet)
	if !IsCeleb(user) {
		mail, _ := json.Marshal(connections.Mail{User: user, Tweet: tweet, Time: now})
		batch.Query("INSERT INTO outbox (username, posted_at, mail) VALUES (?, ?, ?)", user, now, string(mail))
	}
	connections.Cassandra.ExecuteBatch(batch)
}

// sending outbox to kafka
func SendOutbox() {
	// read all 3 replicas. if a node is down, fall back to the ones that are up
	for {
		iter := connections.Cassandra.Query("SELECT username, posted_at, mail FROM outbox").
			Consistency(gocql.All).RetryPolicy(connections.Fallback).Iter()

		var user, mail string
		var postedAt int64
		for iter.Scan(&user, &postedAt, &mail) {
			err := connections.Producer.WriteMessages(connections.Ctx, kafka.Message{
				Key:   []byte(user),
				Value: []byte(mail),
			})
			if err == nil {
				connections.Cassandra.Query("DELETE FROM outbox WHERE username = ? AND posted_at = ?", user, postedAt).Exec()
			}
		}
		iter.Close()
		time.Sleep(10 * time.Second)
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
	//the naive query
	//connections.Mysql.QueryRow("SELECT COUNT(*) FROM follows WHERE target = ?", user).Scan(&count)
	return count
}

func IsCeleb(user string) bool {
	return GetFollowerCount(user) >= CelebThreshold
}

func GetFollowers(user string) []string {
	rows, _ := connections.Mysql.Query("SELECT user FROM follows WHERE target = ?", user)
	//the naive query
	//rows, _ := connections.Mysql.Query("SELECT user FROM follows IGNORE INDEX (target) WHERE target = ?", user)
	
	// converts from mysql rows to go list
	var followers []string
	for rows.Next() {
		var follower string
		rows.Scan(&follower)
		followers = append(followers, follower)
	}
	rows.Close()
	return followers
}

func GetCelebFollows(user string) []string {
	rows, _ := connections.Mysql.Query("SELECT f.target FROM follows f JOIN users u ON u.username = f.target WHERE f.user = ? AND u.follower_count >= ?", user, CelebThreshold)
	var celebs []string
	for rows.Next() {
		var celeb string
		rows.Scan(&celeb)
		celebs = append(celebs, celeb)
	}
	rows.Close()
	return celebs
}

func OpenMail(user string) []string {
	if !UserExists(user) {
		return nil
	}

	//hashset keyed by time and user, to sort by time first
	all := map[string]string{}

	texts, _ := connections.Redis.LRange(connections.Ctx, "mailbox:"+user, 0, -1).Result()
	for _, text := range texts {
		var mail connections.Mail
		json.Unmarshal([]byte(text), &mail)
		all[fmt.Sprintf("%020d:%s", mail.Time, mail.User)] = mail.Tweet
	}

	for _, celeb := range GetCelebFollows(user) {
		var tweet string
		var postedAt int64
		err := connections.Cassandra.Query("SELECT tweet, posted_at FROM posts WHERE username = ? LIMIT 1", celeb).
			Consistency(gocql.All).RetryPolicy(connections.Fallback).Scan(&tweet, &postedAt)
		if err == nil {
			all[fmt.Sprintf("%020d:%s", postedAt, celeb)] = tweet
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