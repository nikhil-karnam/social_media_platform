package connections

import (
	"context"
	"os"
	"time"
	"strings"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gocql/gocql"
	"github.com/segmentio/kafka-go"
	"github.com/redis/go-redis/v9"
)

var Ctx = context.Background()

type Mail struct {
	User  string
	Time  int64
	Tweet string
}

//don't spam retry when redis is clearly down
var Redis = redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR"), MaxRetries: -1})

//connection to cassandra nodes
func Connect(addrs string) *gocql.Session {
	cluster := gocql.NewCluster(strings.Split(addrs, ",")...)
	cluster.Keyspace = "tweets"
	cluster.Consistency = gocql.One
	session, _ := cluster.CreateSession()
	return session
}
var Cassandra = Connect(os.Getenv("CASSANDRA_ADDRS"))
// read all 3 replicas. if a node is down, fall back to the ones that are up
var Fallback = &gocql.DowngradingConsistencyRetryPolicy{ConsistencyLevelsToTry: []gocql.Consistency{gocql.Quorum, gocql.One}}

//connection only to MySQL primary node since that's where writes flow
var Mysql, _ = sql.Open("mysql", os.Getenv("MYSQL_USER")+":"+os.Getenv("MYSQL_PASSWORD")+"@tcp("+os.Getenv("MYSQL_ADDR")+")/"+os.Getenv("MYSQL_DB"))
func init() {
	Mysql.SetMaxOpenConns(100)
}

//connection to kafka
var Producer = &kafka.Writer{
	Addr:         kafka.TCP(strings.Split(os.Getenv("KAFKA_ADDRS"), ",")...),
	Topic:        os.Getenv("KAFKA_TOPIC"),
	//kafka waits to send in batches, default is 1 second wait. change this so a stream of tweets don't wait on each other.
	BatchTimeout: 10 * time.Millisecond,
	// so the same key goes to the same producer
	Balancer:     &kafka.Hash{},
}
