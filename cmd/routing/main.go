package main

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	"github.com/saurav-c/aftsi/config"

	"github.com/golang/protobuf/ptypes/empty"
	pb "github.com/saurav-c/aftsi/proto/routing"
	"google.golang.org/grpc"
)

const (
	TxnRouterPort = ":5006"
	KeyRouterPort = ":5007"
)

func (r *RouterServer) FetchNew(ctx context.Context, emp *empty.Empty) (*pb.RouterResponse, error) {
	fmt.Println("Received request")
	index := rand.Intn(len(r.router))
	ipAddress := r.router[index]
	return &pb.RouterResponse{
		Ip: ipAddress,
	}, nil
}

func (r *RouterServer) LookUp(ctx context.Context, req *pb.RouterReq) (*pb.RouterResponse, error) {
	fmt.Println("Received request")
	keyLookup := req.GetReq()
	h := sha1.New()
	keySha := h.Sum([]byte(keyLookup))
	intSha := binary.BigEndian.Uint64(keySha)
	index := intSha % uint64(len(r.router))
	ipAddress := r.router[index]
	return &pb.RouterResponse{
		Ip: ipAddress,
	}, nil
}

func (r *RouterServer) MultipleLookUp(ctx context.Context, multi *pb.RouterReqMulti) (*pb.MultiRouterResponse, error) {
	fmt.Println("Received request")
	keyLookups := multi.GetReq()
	h := sha1.New()
	ipMap := make(map[string][]string)
	for _, elem := range keyLookups {
		keySha := h.Sum([]byte(elem))
		intSha := binary.BigEndian.Uint64(keySha)
		index := intSha % uint64(len(r.router))
		nodeIP := r.router[index]

		if _, ok := ipMap[nodeIP]; !ok {
			ipMap[nodeIP] = make([]string, 0)
		}
		ipMap[nodeIP] = append(ipMap[nodeIP], elem)
	}

	ipMapResponse := make(map[string]*pb.MultiResponse)
	for ip, set := range ipMap {
		ipMapResponse[ip] = &pb.MultiResponse{
			Resp: set,
		}
	}

	return &pb.MultiRouterResponse{
		IpMap: ipMapResponse,
	}, nil
}

func main() {
	mode := flag.String("mode", "", "Router mode")
	flag.Parse()

	RouterPort := ""
	if *mode == "txn" {
		RouterPort = ":5006"
	} else if *mode == "key" {
		RouterPort = ":5007"
	} else {
		fmt.Println(*mode)
		log.Fatal("Wrong mode")
	}

	configValue := config.ParseConfig()
	IpAddresses := configValue.NodeIPs

	lis, err := net.Listen("tcp", RouterPort)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", RouterPort, err)
	}

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
