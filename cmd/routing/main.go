package main

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	pb "github.com/saurav-c/aftsi/proto/routing/api"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"time"
)

const (
	RouterPort = ":5006"
)

func (r *RouterServer) LookUp(ctx context.Context, req *pb.RouterReq) (*pb.RouterResponse, error) {
	keyLookup := req.GetReq()
	h := sha1.New()
	keySha := h.Sum([]byte(keyLookup))
	intSha := binary.BigEndian.Uint64(keySha)
	index := intSha % uint64(len(r.router))
	ipAddress := r.router[index]
	return &pb.RouterResponse{
		Ip:    ipAddress,
	}, nil
}

func (r *RouterServer) MultipleLookUp(ctx context.Context, multi *pb.RouterReqMulti) (*pb.MultiRouterResponse, error) {
	keyLookups := multi.GetReq()
	h := sha1.New()
	ipAddrs := make([]string, len(keyLookups))
	for indexKey, elem := range keyLookups {
		keySha := h.Sum([]byte(elem))
		intSha := binary.BigEndian.Uint64(keySha)
		index := intSha % uint64(len(r.router))
		ipAddrs[indexKey] = r.router[index]
	}
	return &pb.MultiRouterResponse{
		Ip: ipAddrs,
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", RouterPort)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", RouterPort, err)
	}
	IpAddresses := os.Args[1:]

	server := grpc.NewServer()

	router, err := NewRouter(IpAddresses)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", RouterPort, err)
	}
	pb.RegisterRouterServer(server, router)

	fmt.Printf("Starting server at %s.\n", time.Now().String())
	if err = server.Serve(lis); err != nil {
		log.Fatal("Could not start server on port %s: %v\n", RouterPort, err)
	}
}