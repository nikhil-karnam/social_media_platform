package main

import (
	"context"
	"net"
	"os"
	"slices"

	"github.com/cockroachdb/pebble"
	"google.golang.org/grpc"
	"twitter_clone/functionality"
	pb "twitter_clone/tweets"
)

type server struct {
	pb.UnimplementedTweetsServer
	db *pebble.DB
}

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutReply, error) {
	return &pb.PutReply{}, s.db.Set(req.Key, req.Value, pebble.Sync)
}

func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.PutReply, error) {
	return &pb.PutReply{}, s.db.Delete(req.Key, pebble.Sync)
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetReply, error) {
	value, closer, err := s.db.Get(req.Key)
	if err == pebble.ErrNotFound {
		return &pb.GetReply{Found: false}, nil
	}
	defer closer.Close()
	return &pb.GetReply{Found: true, Value: slices.Clone(value)}, nil
}

func (s *server) Scan(req *pb.ScanRequest, stream pb.Tweets_ScanServer) error {
	iter, _ := s.db.NewIter(&pebble.IterOptions{LowerBound: req.Lower, UpperBound: req.Upper})
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		stream.Send(&pb.KV{Key: slices.Clone(iter.Key()), Value: slices.Clone(iter.Value())})
	}
	return nil
}

func (s *server) PutBatch(ctx context.Context, req *pb.PutBatchRequest) (*pb.PutReply, error) {
	batch := s.db.NewBatch()
	defer batch.Close()
	for _, kv := range req.Kvs {
		batch.Set(kv.Key, kv.Value, nil)
	}
	return &pb.PutReply{}, batch.Commit(pebble.Sync)
}

func (s *server) Last(ctx context.Context, req *pb.ScanRequest) (*pb.LastReply, error) {
	iter, _ := s.db.NewIter(&pebble.IterOptions{LowerBound: req.Lower, UpperBound: req.Upper})
	defer iter.Close()
	if !iter.Last() {
		return &pb.LastReply{Found: false}, nil
	}
	return &pb.LastReply{Found: true, Kv: &pb.KV{Key: slices.Clone(iter.Key()), Value: slices.Clone(iter.Value())}}, nil
}

// usage: go run ./cmd/server data/tweets0 :50051
func main() {
	lsm, _ := pebble.Open(os.Args[1], &pebble.Options{})
	defer lsm.Close()
	listener, _ := net.Listen("tcp", os.Args[2])
	network := grpc.NewServer()
	tweets := &server{db: lsm}
	go functionality.SendOutbox(lsm)
	pb.RegisterTweetsServer(network, tweets)
	network.Serve(listener)
}