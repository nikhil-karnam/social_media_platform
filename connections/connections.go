package connections

import (
	"context"
	"os"
	"time"
	"strings"
	
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "twitter_clone/tweets"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/segmentio/kafka-go"
	"github.com/redis/go-redis/v9"
)

var Ctx = context.Background()

type Mail struct {
	User  string
	Time  int64
	Tweet string
}

//connects to one gRPC server by address
func Connect(addr string) pb.TweetsClient {
	conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return pb.NewTweetsClient(conn)
}
//connects to one mysql node by address
func Open(addr string) *sql.DB {
	db, _ := sql.Open("mysql", "root:"+os.Getenv("MYSQL_PASSWORD")+"@tcp("+addr+")/twitter")
	return db
}

//we only need to connect to the primary port for reads and writes.
//when primary dies, another surviving node will automatically get rotated to primary port.
var Mysql = Open(os.Getenv("MYSQL_ADDR"))
//queue queries past 100 so MySQL can handle the incoming load
func init() {
	Mysql.SetMaxOpenConns(100)
}

var Tweets = [3]pb.TweetsClient{
	Connect(os.Getenv("TWEET0_ADDR")),
	Connect(os.Getenv("TWEET1_ADDR")),
	Connect(os.Getenv("TWEET2_ADDR")),
}
//kafka waits to send in batches, default is 1 second wait. change this so a stream of tweets don't wait on each other.
var Producer = &kafka.Writer{
	Addr:         kafka.TCP(strings.Split(os.Getenv("KAFKA_ADDRS"), ",")...),
	Topic:        "fanout",
	BatchTimeout: 10 * time.Millisecond,
	// so the same key goes to the same producer
	Balancer:     &kafka.Hash{},
}
//don't spam retry when redis is clearly down
var Redis = redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR"), MaxRetries: -1})