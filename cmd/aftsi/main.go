package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"math/rand"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/proto"
	pb "github.com/saurav-c/aftsi/proto/aftsi/api"

	// rpb "github.com/saurav-c/aftsi/proto/aftsi/replica"
	"google.golang.org/grpc"
)

const (
	TxnServerPort = ":5000"
)

func (s *AftSIServer) StartTransaction(ctx context.Context, emp *empty.Empty) (*pb.TransactionID, error) {
	// Generate TID
	s.counterMutex.Lock()
	tid := s.serverID + strconv.Iota(s.counter)
	s.counter += 1
	s.counterMutex.Unlock()

	// Ask router for the IP address of master for this TID
	txnManagerIP, err := txnMa(s.zmqInfo, tid)
	if err != nil {
		return &pb.TransactionID{tid: "", e: pb.TransactionError_FAILURE}, nil
	}

	// Use GRPC to make internal request to this TID's Txn Manager
	conn, err := grpc.Dial(txnManagerIP + ":" + TxnServerPort)
	if err != nil {
		return &pb.TransactionID{tid: "", e: pb.TransactionError_FAILURE}, nil
	}
	defer conn.Close()

	client := pb.NewAftSIClient(conn)
	_, err := client.CreateTransactionEntry(context.Context, &pb.TransactionID{tid: tid})
	if err != nil {
		return &pb.TransactionID{tid: "", e: pb.TransactionError_FAILURE}, nil
	}

	return &pb.TransactionID{tid: tid, e: pb.TransactionError_SUCCESS}, nil
}

// TODO: Fetch from read cache and update read cache
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
			return nil, &pb.TransactionResponse{value: val, e: pb.TransactionError_SUCCESS}
		}
	}
	s.WriteBufferLock.RUnlock()

	// Reading from the ReadSet
	s.TransactionTableLock[tid].Lock()
	readSet := s.TransactionTable[tid].readSet
	s.TransactionTable[tid].Unlock()
	versionedKey, ok := readSet[key]
	if ok { 
		// Fetch correct version from ReadSet
		s.ReadCacheLock.RLock()
		if val, ok := s.ReadCache[versionedKey]; ok {
			s.ReadCacheLock.RUnlock()
			return nil, &pb.TransactionResponse{value: val, e: pb.TransactionError_SUCCESS}
		}
		s.ReadCacheLock.RUnlock()
		// Fetch Value From Storage (stored in val)
		s.ReadCacheLock.Lock()
		if len(s.ReadCache) == main.ReadCacheLimit {
			randInt := rand.Intn(len(s.ReadCache))
			randKey := ""
			for key := range sammy {
				randKey = key
				if (randInt == 0) {
					break
				}
				randInt -= 1
			}
			del(s.ReadCache, randKey)
		}
		s.ReadCacheLock.Unlock()
		s.ReadCache[versionedKey] = val
	}

	// Fetch Correct Version of Key from KeyNode and read from Storage
	s.ReadCacheLock.Lock()
	if len(s.ReadCache) == main.ReadCacheLimit {
		randInt := rand.Intn(len(s.ReadCache))
		randKey := ""
		for key := range sammy {
			randKey = key
			if (randInt == 0) {
				break
			}
			randInt -= 1
		}
		del(s.ReadCache, randKey)
	}
	s.ReadCacheLock.Unlock()
	s.ReadCache[versionedKey] = val

	// Make routing request to find Key Node IP. Use ZMQ to lookup key from Key Node

	return nil, &pb.TransactionResponse{value: nil, e: pb.TransactionError_SUCCESS}
}

func (s *AftSIServer) Write(ctx context.Context, writeReq *pb.WriteRequest) (*pb.TransactionResponse, error) {
	// Parse read request fields
	tid := writeReq.GetTid()
	key := writeReq.GetKey()
	val := writeReq.GetValue()

	s.WriteBufferLock[tid].Lock()
	s.WriteBuffer[tid][key] = val
	s.WriteBufferLock[tid].Unlock()

	// Send this to the Child Nodes

	return &pb.TransactionResponse{value: nil, e: pb.TransactionError_SUCCESS}, nil
}

func (s *AftSIServer) CommitTransaction(ctx context.Context, tid *pb.TransactionID) (*pb.TransactionResponse, error) {
	// send internal validate(TID, writeSet, Begin-TS, Commit-TS) to all keyNodes
	panic("implement me")
}

func (s *AftSIServer) AbortTransaction(ctx context.Context, tid *pb.TransactionID) (*pb.TransactionResponse, error) {
	pbtid := tid.GetTid()
	tidLock, ok := s.TransactionTableLock[pbtid]
	if ok {
		tidLock.Lock()
		_, ok = s.TransactionTable[pbtid];
		if ok {
				delete(s.TransactionTable, pbtid);
		}
		else {
			return &pb.TransactionResponse{value: nil, e: pb.TransactionError_FAILURE}, errors.New("Couldn't find entry in transaction table.")
		}
		tidLock.Unlock()
		delete(s.TransactionTableLock, pbtid)
	}
	else {
		return &pb.TransactionResponse{value: nil, e: pb.TransactionError_FAILURE}, errors.New("Couldn't find lock for transaction table entry.")
	}
	writeBufferLock, ok := s.WriteBufferLock[pbtid]
	if ok {
		s.writeBufferLock.Lock()
		_, ok = s.WriteBuffer[pbtid];
		if ok {
				delete(s.WriteBuffer, pbtid);
		}
		else {
			return &pb.TransactionResponse{value: nil, e: pb.TransactionError_FAILURE}, errors.New("Couldn't find entry in write buffer.")
		}
		s.writeBufferLock.Unlock()
		delete(s.writeBufferLock, pbtid)
	}
	else {
		return &pb.TransactionResponse{value: nil, e: pb.TransactionError_FAILURE}, errors.New("Couldn't find lock for write buffer entry.")
	}
	return &pb.TransactionResponse{value: nil, e: pb.TransactionError_SUCCESS}, nil
}

func (s *AftSIServer) CreateTransactionEntry(ctx context.Context, entry *pb.NewTransactionEntry) (*empty.Empty, error) {
	clientIP := entry.GetClientIP()
	tid := entry.GetTid()
	s.TransactionTable[tid] = TransactionEntry{BeginTS: time.Now().String(),coWrittenSets: make(map[string][]string), unverifiedProtos: make(map[hash.Hash])}
	s.WriteBuffer[tid] = make(map[string]byte)

	// Send Response to Client 

	return empty.Empty{}, nil
}

func main() {
	lis, err := net.Listen("tcp", TxnServerPort)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", port, err)
	}

	// TODO: Figure out actual AFTSI server setup calls
	server := grpc.NewServer()
	aftsi, config, err := NewAftSIServer()
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", port, err)
	}
	pb.RegisterAftSIServer(server, aftsi)

	fmt.Printf("Starting server at %s.\n", time.Now().String())
	if err = server.Serve(lis); err != nil {
		log.Fatal("Could not start server on port %s: %v\n", port, err)
	}
}