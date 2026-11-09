package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"twitter_clone/connections"
	"twitter_clone/functionality"
)

//reads tweets from kafka and delivers them to mailboxes
func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(os.Getenv("KAFKA_ADDRS"), ","),
		Topic:   os.Getenv("KAFKA_TOPIC"),
		GroupID: os.Getenv("KAFKA_GROUP"),
	})
	defer reader.Close()

	for {
		//fetch works on the next mail without moving the kafka checkpoint
		msg, _ := reader.FetchMessage(connections.Ctx)

		var mail connections.Mail
		//unmarshal just to find the followers to send the mail to
		json.Unmarshal(msg.Value, &mail)
		key := fmt.Sprintf("delivered:%s:%020d", mail.User, mail.Time)

		//watch the key: if another worker saves it first, our pipeline gets cancelled
		err := connections.Redis.Watch(connections.Ctx, func(tx *redis.Tx) error {
			//already delivered, skip it
			if tx.Exists(connections.Ctx, key).Val() == 1 {
				return nil
			}

			pipe := tx.TxPipeline()
			for _, follower := range functionality.GetFollowers(mail.User) {
				pipe.RPush(connections.Ctx, "mailbox:"+follower, msg.Value)
				pipe.LTrim(connections.Ctx, "mailbox:"+follower, -20, -1)
			}
			// set the value of the idempotency key as how long it took to deliver
			pipe.Set(connections.Ctx, key, time.Since(msg.Time).Nanoseconds(), 24*time.Hour)

			_, err := pipe.Exec(connections.Ctx)
			return err
		}, key)

		//quit only if redis is down and if the cause is not due to the optimistic lock
		if err != nil && err != redis.TxFailedErr {
			return
		}
		
		//only move the checkpoint once every mailbox got the tweet
		reader.CommitMessages(connections.Ctx, msg)
	}
}