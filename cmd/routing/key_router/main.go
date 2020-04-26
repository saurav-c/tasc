package main

import (
	"context"
	"crypto/sha1"
	"fmt"
	pb "github.com/saurav-c/aftsi/proto/routing/api"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"time"
)

const (
	KeyRouterPort = ":5006"
)

func _convertByteToInt(arr []byte) (int) {
	retVal := 0
	for _, elem := range arr {
		retVal = retVal + int(elem)
	}
	return retVal
}

func (k *KeyRouterServer) KeyLookup(ctx context.Context, req *pb.KeyRouterReq) (*pb.RouterResponse, error){
	keyLookup := req.GetKey()
	h := sha1.New()
	keySha := h.Sum([]byte(keyLookup))
	index := _convertByteToInt(keySha) % len(k.router)
	ipAddress := k.router[index]
	return &pb.RouterResponse{
		Ip:    ipAddress,
		Error: 0,
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", KeyRouterPort)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", KeyRouterPort, err)
	}
	IpAddresses := os.Args[1:]

	server := grpc.NewServer()

	keyRouter, err := NewKeyRouter(IpAddresses)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", KeyRouterPort, err)
	}
	pb.RegisterKeyRouterServer(server, keyRouter)

	fmt.Printf("Starting server at %s.\n", time.Now().String())
	if err = server.Serve(lis); err != nil {
		log.Fatal("Could not start server on port %s: %v\n", KeyRouterPort, err)
	}
}