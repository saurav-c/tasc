package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	pb "github.com/saurav-c/aftsi/proto"
	"google.golang.org/grpc"
)

const (
	port = ":7654"
)

func (s *AftSIServer) StartTransaction(ctx context.Context, *empty.Empty) (*pb.TransactionID, error) {
	// Generate TID
	s.counterMutex.Lock()
	tid := s.serverID + strconv.Iota(s.counter)
	s.counter += 1
	s.counterMutex.Unlock()

	// Use ZMQ to make blocking createTxnTableEntry request
	// Do any error handling if necessary

	return &pb.TransactionID{tid: tid, e: pb.TransactionError_SUCCESS}, nil
}

func (s *AftSIServer) Read(ctx context.Context, readReq *pb.ReadRequest) (*pb.TransactionResponse, error) {
	// Parse read request fields
	tid := readReq.GetTid()
	key := readReq.GetKey()
	reqOpCounter := readReq.GetOpCounter()

	s.TransactionTableLock.RLock()
	opCounter := s.TransactionTable[tid]
	s.TransactionTableLock.RUnlock()

	if opCounter != reqOpCounter {
		// Need to double check
		_, err := s.AbortTransaction(ctx, &pb.TransactionID{id: tid})
		if err != nil {
			// internal error processing
		}
		return nil, errors.New("Transaction had to be aborted")
	}

	// Reading from the Write Buffer
	s.WriteBufferLock.RLock()
	if writeBuf, ok := s.WriteBuffer[tid]; ok {
		if val, ok := writeBuf[key]; ok {
			s.WriteBufferLock.RUnlock()
			return &pb.TransactionResponse{value: val, e: pb.TransactionError_SUCCESS}
		}
	}
	s.WriteBufferLock.RUnlock()

	// Reading from the ReadSet
	// Fetch correct version from ReadSet
	// key := keyVersion
	s.ReadCacheLock.RLock()
	if val, ok := s.ReadCache[key]; ok {
		s.ReadCacheLock.RUnlock()
		return &pb.TransactionResponse{value: val, e: pb.TransactionError_SUCCESS}
	}
	s.ReadCacheLock.RUnlock()
	// Otherwise, read from storage layer

	// Make routing request to find Key Node IP. Use ZMQ to lookup key from Key Node

	return &pb.TransactionResponse{value: nil, e: pb.TransactionError_SUCCESS}
}

func (s *AftSIServer) Write(ctx context.Context, writeReq *pb.WriteRequest) (*pb.TransactionResponse, error) {
	// Parse read request fields
	tid := writeReq.GetTid()
	key := writeReq.GetKey()
	val := writeReq.GetValue()
	reqOpCounter := writeReq.GetOpCounter()

	s.TransactionTableLock.RLock()
	opCounter := s.TransactionTable[tid]
	s.TransactionTableLock.RUnlock()

	if opCounter != reqOpCounter {
		// Need to double check
		_, err := s.AbortTransaction(ctx, &pb.TransactionID{id: tid})
		if err != nil {
			// internal error processing
		}
		return nil, errors.New("Transaction had to be aborted")
	}

	s.WriteBufferLock.Lock()
	s.WriteBuffer[tid][key] = val
	s.WriteBufferLock.Unlock()
	return &pb.TransactionResponse{value: nil, e: pb.TransactionError_SUCCESS}, nil
}

func (s *AftSIServer) CommitTransaction(ctx context.Context, tid *pb.TransactionID) (*pb.TransactionResponse, error) {
	panic("implement me")
}

func (s *AftSIServer) AbortTransaction(ctx context.Context, tid *pb.TransactionID) (*pb.TransactionResponse, error) {
	panic("implement me")
}

func (s *AftSIServer) CreateTransactionEntry(ctx ontext.Context, entry *pb.NewTransactionEntry) (*empty.Empty, error) {
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

	fmt.Printf("Starting server at %s.\n", time.Now().String())
	if err = server.Serve(lis); err != nil {
		log.Fatal("Could not start server on port %s: %v\n", port, err)
	}
}
