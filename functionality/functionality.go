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
	"sync"

	"twitter_clone/connections"

	"github.com/gocql/gocql"
	"github.com/segmentio/kafka-go"
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
	mail, _ := json.Marshal(connections.Mail{User: user, Tweet: tweet, Time: now})
	if !IsCeleb(user) {
		batch.Query("INSERT INTO outbox (username, posted_at, mail) VALUES (?, ?, ?)", user, now, string(mail))
	} else {
		batch.Query("INSERT INTO celeb_outbox (username, posted_at, mail) VALUES (?, ?, ?)", user, now, string(mail))
	}

	connections.Cassandra.ExecuteBatch(batch)
}

// sending outbox to kafka
func SendOutbox() {
	for {
		// read all 3 replicas. if that fails, fall back to 2, then 1
		iter := connections.Cassandra.Query("SELECT username, posted_at, mail FROM outbox").
			Consistency(gocql.All).RetryPolicy(connections.Fallback).Iter()

		// collect everything waiting in the outbox first
		var msgs []kafka.Message
		var rows [][]any
		var user, mail string
		var postedAt int64
		for iter.Scan(&user, &postedAt, &mail) {
			msgs = append(msgs, kafka.Message{Key: []byte(user), Value: []byte(mail)})
			rows = append(rows, []any{user, postedAt})
		}
		iter.Close()

		// send everything in one go. if kafka fails, the mail stays in the outbox and gets retried next loop
		if len(msgs) > 0 && connections.Producer.WriteMessages(connections.Ctx, msgs...) == nil {
			// kafka confirmed it all, so delete those rows from the outbox all at once
			var wg sync.WaitGroup
			for _, r := range rows {
				wg.Go(func() {
					connections.Cassandra.Query("DELETE FROM outbox WHERE username = ? AND posted_at = ?", r...).Exec()
				})
			}
			wg.Wait()
		}

		time.Sleep(10 * time.Second)
	}
}

// saving each celebrity's newest post to redis
func SendCelebOutbox() {
	for {
		// read all 3 replicas. if that fails, fall back to 2, then 1
		iter := connections.Cassandra.Query("SELECT username, posted_at, mail FROM celeb_outbox").
			Consistency(gocql.All).RetryPolicy(connections.Fallback).Iter()

		var pairs []any
		var rows [][]any
		var user, mail string
		var postedAt int64
		for iter.Scan(&user, &postedAt, &mail) {
			pairs = append(pairs, user, mail)
			rows = append(rows, []any{user, postedAt})
		}
		iter.Close()

		// send everything in one go. if redis fails, the mail stays in the outbox and gets retried next loop
		if len(pairs) > 0 && connections.CelebRedis.MSet(connections.Ctx, pairs...).Err() == nil {
			// redis confirmed it all, so delete those rows from the outbox all at once
			var wg sync.WaitGroup
			for _, r := range rows {
				wg.Go(func() {
					connections.Cassandra.Query("DELETE FROM celeb_outbox WHERE username = ? AND posted_at = ?", r...).Exec()
				})
			}
			wg.Wait()
		}

		time.Sleep(10 * time.Second)
	}
}

// getters
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

// get all* follows
func GetFollows(user string) []string {
	rows, _ := connections.Mysql.Query("SELECT target FROM follows WHERE user = ?", user)
	var follows []string
	for rows.Next() {
		var target string
		rows.Scan(&target)
		follows = append(follows, target)
	}
	rows.Close()
	return follows
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

	posts, _ := connections.CelebRedis.MGet(connections.Ctx, GetCelebFollows(user)...).Result()
	for _, p := range posts {
		if p != nil {
			var mail connections.Mail
			json.Unmarshal([]byte(p.(string)), &mail)
			all[fmt.Sprintf("%020d:%s", mail.Time, mail.User)] = mail.Tweet
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

// reads every followed user's latest post straight from cassandra. no mailbox, no cache
func DumpsterfireOpenMail(user string) []string {
	if !UserExists(user) {
		return nil
	}

	all := map[string]string{}

	for _, target := range GetFollows(user) {
		var tweet string
		var postedAt int64
		err := connections.Cassandra.Query("SELECT tweet, posted_at FROM posts WHERE username = ? LIMIT 1", target).
			Consistency(gocql.Quorum).RetryPolicy(connections.Fallback).Scan(&tweet, &postedAt)
		if err == nil {
			all[fmt.Sprintf("%020d:%s", postedAt, target)] = tweet
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