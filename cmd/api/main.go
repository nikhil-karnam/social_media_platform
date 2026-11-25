package main

import (
	"context"
	"net"
	"os"
	"sync"

	"google.golang.org/grpc"

	pb "twitter_clone/api"
	"twitter_clone/functionality"
)

type server struct {
	pb.UnimplementedApiServer
}

// setters
func (s *server) Signup(ctx context.Context, req *pb.Accounts) (*pb.Empty, error) {
	functionality.SignupBatch(req.Users, req.Emails)
	return &pb.Empty{}, nil
}

func (s *server) Follow(ctx context.Context, req *pb.Follows) (*pb.Empty, error) {
	functionality.FollowBatch(req.Users, req.Targets)
	return &pb.Empty{}, nil
}

// for determining who is celeb
func (s *server) SetCelebThreshold(ctx context.Context, req *pb.Threshold) (*pb.Empty, error) {
	functionality.CelebThreshold = int(req.Value)
	return &pb.Empty{}, nil
}

func (s *server) BurstTweet(ctx context.Context, req *pb.Tweets) (*pb.Empty, error) {
	var wg sync.WaitGroup
	for i := range req.Users {
		wg.Go(func() { functionality.Tweet(req.Users[i], req.Tweets[i]) })
	}
	wg.Wait()
	return &pb.Empty{}, nil
}

// getters
func (s *server) OpenMail(ctx context.Context, req *pb.Username) (*pb.Timeline, error) {
	return &pb.Timeline{Tweets: functionality.OpenMail(req.User)}, nil
}

func (s *server) DumpsterfireOpenMail(ctx context.Context, req *pb.Username) (*pb.Timeline, error) {
	return &pb.Timeline{Tweets: functionality.DumpsterfireOpenMail(req.User)}, nil
}

func main() {
	go functionality.SendOutbox()
	go functionality.SendCelebOutbox()

	lis, _ := net.Listen("tcp", os.Getenv("API_ADDR"))
	s := grpc.NewServer()
	pb.RegisterApiServer(s, &server{})
	s.Serve(lis)
}
