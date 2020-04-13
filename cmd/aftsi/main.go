package main

import (
	"context"
	"errors"
	"fmt"
	"hash"
	"log"
	"net"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	// "github.com/golang/protobuf/proto"
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
	tid := s.serverID + strconv.FormatUint(s.counter, 10)
	s.counter += 1
	s.counterMutex.Unlock()

	// Ask router for the IP address of master for this TID
	txnManagerIP, err := pingTxnRouter(&s.zmqInfo, tid)
	if err != nil {
		return &pb.TransactionID{
			Tid: "",
			E:   pb.TransactionError_FAILURE,
		}, nil
	}

	// Use GRPC to make internal request to this TID's Txn Manager
	conn, err := grpc.Dial(txnManagerIP + ":" + TxnServerPort)
	if err != nil {
		return &pb.TransactionID{
			Tid: "",
			E:   pb.TransactionError_FAILURE,
		}, nil
	}
	defer conn.Close()

	client := pb.NewAftSIClient(conn)
	_, err = client.CreateTransactionEntry(context.Background(), &pb.TransactionID{Tid: tid,})
	if err != nil {
		return &pb.TransactionID{
			Tid: "",
			E:   pb.TransactionError_FAILURE,
		}, nil
	}

	return &pb.TransactionID{
		Tid: tid,
		E:   pb.TransactionError_SUCCESS,
	}, nil
}

// TODO: Fetch from read cache and update read cache
func (s *AftSIServer) Read(ctx context.Context, readReq *pb.ReadRequest) (*pb.TransactionResponse, error) {
	// Parse read request fields
	tid := readReq.GetTid()
	key := readReq.GetKey()

	// Verify transaction status
	s.TransactionTableLock[tid].RLock()
	if entry, ok := s.TransactionTable[tid]; !ok || entry.status != TxnInProgress {
		s.TransactionTableLock[tid].RUnlock()
		return &pb.TransactionResponse{
			E: pb.TransactionError_FAILURE,
		}, nil
	}
	s.TransactionTableLock[tid].RUnlock()

	// Reading from the Write Buffer
	s.WriteBufferLock[tid].RLock()
	if writeBuf, ok := s.WriteBuffer[tid]; ok {
		if val, ok := writeBuf[key]; ok {
			s.WriteBufferLock[tid].RUnlock()
			return &pb.TransactionResponse{
				Value: val,
				E:     pb.TransactionError_SUCCESS,
			}, nil
		}
	}
	s.WriteBufferLock[tid].RUnlock()

	// Reading from the ReadSet
	s.TransactionTableLock[tid].RLock()
	readSet := s.TransactionTable[tid].readSet
	s.TransactionTableLock[tid].RUnlock()
	versionedKey, ok := readSet[key]
	if ok { 
		// Fetch correct version from ReadSet, check for version in ReadCache
		s.ReadCacheLock.RLock()
		if val, ok := s.ReadCache[versionedKey]; ok {
			s.ReadCacheLock.RUnlock()
			return &pb.TransactionResponse{
				Value: val,
				E:     pb.TransactionError_SUCCESS,
			}, nil
		}
		s.ReadCacheLock.RUnlock()
		// Fetch Value From Storage (stored in val)
		// TODO
		var val []byte

		s.ReadCacheLock.Lock()
		if len(s.ReadCache) == ReadCacheLimit {
			randInt := rand.Intn(len(s.ReadCache))
			key := ""
			for key = range s.ReadCache {
				if (randInt == 0) {
					break
				}
				randInt -= 1
			}
			delete(s.ReadCache, key)
		}
		s.ReadCache[versionedKey] = val
		s.ReadCacheLock.Unlock()

		return &pb.TransactionResponse{
			Value: val,
			E:     pb.TransactionError_SUCCESS,
		}, nil
	}

	// Fetch Correct Version of Key from KeyNode and read from Storage
	// TODO


	var val []byte
	s.ReadCacheLock.Lock()
	if len(s.ReadCache) == ReadCacheLimit {
		randInt := rand.Intn(len(s.ReadCache))
		key := ""
		for key = range s.ReadCache {
			if (randInt == 0) {
				break
			}
			randInt -= 1
		}
		delete(s.ReadCache, key)
	}
	s.ReadCache[versionedKey] = val
	s.ReadCacheLock.Unlock()

	return &pb.TransactionResponse{
		Value: val,
		E:     pb.TransactionError_SUCCESS,
	}, nil
}

func (s *AftSIServer) Write(ctx context.Context, writeReq *pb.WriteRequest) (*pb.TransactionResponse, error) {
	// Parse read request fields
	tid := writeReq.GetTid()
	key := writeReq.GetKey()
	val := writeReq.GetValue()

	// Verify transaction status
	s.TransactionTableLock[tid].RLock()
	if entry, ok := s.TransactionTable[tid]; !ok || entry.status != TxnInProgress {
		s.TransactionTableLock[tid].RUnlock()
		return &pb.TransactionResponse{
			E: pb.TransactionError_FAILURE,
		}, nil
	}
	s.TransactionTableLock[tid].RUnlock()

	s.WriteBufferLock[tid].Lock()
	s.WriteBuffer[tid][key] = val
	s.WriteBufferLock[tid].Unlock()

	// Send update to replicas
	// TODO

	return &pb.TransactionResponse{
		E: pb.TransactionError_SUCCESS,
	}, nil
}

func (s *AftSIServer) CommitTransaction(ctx context.Context, req *pb.TransactionID) (*pb.TransactionResponse, error) {
	// send internal validate(TID, writeSet, Begin-TS, Commit-TS) to all keyNodes
	panic("implement me")
}

func (s *AftSIServer) AbortTransaction(ctx context.Context, req *pb.TransactionID) (*pb.TransactionResponse, error) {
	tid := req.GetTid()
	if tidLock, ok := s.TransactionTableLock[tid]; ok {
		tidLock.Lock()
		defer tidLock.Unlock()
		if entry, ok := s.TransactionTable[tid]; ok && entry.status == TxnInProgress {
			entry.status = TxnAborted
			return &pb.TransactionResponse{
				E: pb.TransactionError_SUCCESS,
			}, nil
		} else {
			// Transaction Entry does not exist or Txn was not In Progress
			return &pb.TransactionResponse{
				E: pb.TransactionError_FAILURE,
			}, nil
		}
	} else {
		// Transaction does not exist
		return &pb.TransactionResponse{
			E: pb.TransactionError_FAILURE,
		}, nil
	}
}

func (s *AftSIServer) CreateTransactionEntry(ctx context.Context, req *pb.TransactionID) (*empty.Empty, error) {
	tid := req.GetTid()

	s.TransactionTable[tid] = &TransactionEntry{
		beginTS: time.Now().String(),
		readSet: make(map[string]string),
		coWrittenSets: make(map[string][]string),
		status: TxnInProgress,
	}
	s.WriteBuffer[tid] = make(map[string][]byte)

	s.TransactionTableLock[tid] = &sync.RWMutex{}
	s.WriteBufferLock[tid] = &sync.RWMutex{}

	return &empty.Empty{}, nil
}

func main() {
	lis, err := net.Listen("tcp", TxnServerPort)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", TxnServerPort, err)
	}

	server := grpc.NewServer()
	// TODO: Lookup router IP Address
	router := ""

	aftsi, _, err := NewAftSIServer(router)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", TxnServerPort, err)
	}
	pb.RegisterAftSIServer(server, aftsi)

	fmt.Printf("Starting server at %s.\n", time.Now().String())
	if err = server.Serve(lis); err != nil {
		log.Fatal("Could not start server on port %s: %v\n", TxnServerPort, err)
	}
}