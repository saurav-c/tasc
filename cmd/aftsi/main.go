package main

import (
	"net"
	"fmt"
	"context"
	"log"

	pb "github.com/saurav-c/aftsi/proto"
	"google.golang.org/grpc"
	"github.com/golang/protobuf/ptypes/empty"
)

const (
	port = ":7654"
)

type TransactionMngrEntry struct {

}

func (s *AftSIServer) StartTransaction(context.Context, *empty.Empty) (*pb.TransactionID, error) {
	panic("implement me")
}

func (s *AftSIServer) Read(context.Context, *pb.ReadRequest) (*pb.TransactionResponse, error) {
	panic("implement me")
}

func (s *AftSIServer) Write(context.Context, *pb.WriteRequest) (*pb.TransactionResponse, error) {
	panic("implement me")
}

func (s *AftSIServer) CommitTransaction(context.Context, *pb.TransactionID) (*pb.TransactionResponse, error) {
	panic("implement me")
}

func (s *AftSIServer) AbortTransaction(context.Context, *pb.TransactionID) (*pb.TransactionResponse, error) {
	panic("implement me")
}


func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", port, err)
	}

	// TODO: Figure out actual AFTSI server setup calls
	server := grpc.NewServer()
	aftsi, config := NewAftSIServer()
	pb.RegisterAftSIServer(server, aftsi)

}
