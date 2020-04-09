package main

import (
	"net"
	"fmt"
	"log"

	pb "github.com/saurav-c/aftsi/proto/aftsi"

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

const (
	port = ":7654"
)

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", port, err)
	}

	server := grpc.NewServer()

}
