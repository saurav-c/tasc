package main

import (
	"context"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
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

	// Use ZMQ to make internal request to this TID's Txn Manager
	createTxnPusher := createSocket(zmq.PUSH, s.zmqInfo.context, fmt.Sprintf(PushTemplate, txnManagerIP, createTxnPortReq), false)
	createNewTxnResponse := createSocket(zmq.PULL, s.zmqInfo.context, fmt.Sprintf(PullTemplate, createTxnPortResp), true)
	defer createTxnPusher.Close()
	defer createNewTxnResponse.Close()

	txnEntryReq := &pb.CreateTxnEntry{
		Tid:          tid,
		TxnManagerIP: s.IPAddress,
	}
	data, _ := proto.Marshal(txnEntryReq)
	createTxnPusher.SendBytes(data, zmq.DONTWAIT)

	// Wait for response
	resp := &pb.TransactionResponse{}
	data, _ = createNewTxnResponse.RecvBytes(0)
	err = proto.Unmarshal(data, resp)
	if err != nil || resp.GetE() != pb.TransactionError_SUCCESS {
		return &pb.TransactionID{
			E: pb.TransactionError_FAILURE,
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
				if randInt == 0 {
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

	// Find lower bound for key version in order to maintain an atomic read set
	// TODO
	// Go thru each cowritten set and check for this key, find max version and send it to KN
	s.TransactionTableLock[tid].RLock()
	coWrittenSet := s.TransactionTable[tid].coWrittenSet
	s.TransactionTableLock[tid].RUnlock()
	keyLowerBound := ""
	if version, ok := coWrittenSet[key]; ok {
		keyLowerBound = version
	}

	// Fetch Correct Version of Key from KeyNode and read from Storage
	// Get Key Node for this key
	keyIP, err := pingKeyRouter(&s.zmqInfo, key)
	if err != nil {
		return &pb.TransactionResponse{
			Value: nil,
			E:     pb.TransactionError_FAILURE,
		}, nil
	}
	// Use ZMQ to make a read request to the Key Node at keyIP
	// TODO

	var val []byte
	s.ReadCacheLock.Lock()
	if len(s.ReadCache) == ReadCacheLimit {
		randInt := rand.Intn(len(s.ReadCache))
		key := ""
		for key = range s.ReadCache {
			if randInt == 0 {
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

func (s *AftSIServer) CreateTransactionEntry(tid string, txnManagerIP string) () {

	s.TransactionTable[tid] = &TransactionEntry{
		beginTS:      time.Now().String(),
		readSet:      make(map[string]string),
		coWrittenSet: make(map[string]string),
		status:       TxnInProgress,
	}
	s.WriteBuffer[tid] = make(map[string][]byte)

	s.TransactionTableLock[tid] = &sync.RWMutex{}
	s.WriteBufferLock[tid] = &sync.RWMutex{}

	resp := &pb.TransactionResponse{
		E: pb.TransactionError_SUCCESS,
	}
	data, _ := proto.Marshal(resp)

	socket := createSocket(zmq.PUSH, s.zmqInfo.context, fmt.Sprintf(PushTemplate, txnManagerIP, createTxnPortResp), false)
	defer socket.Close()
	socket.SendBytes(data, 0)
}

func main() {
	lis, err := net.Listen("tcp", TxnServerPort)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", TxnServerPort, err)
	}

	server := grpc.NewServer()
	// TODO: Lookup router IP Address
	txnRouter := ""
	keyRouter := ""

	aftsi, _, err := NewAftSIServer(txnRouter, keyRouter)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", TxnServerPort, err)
	}
	pb.RegisterAftSIServer(server, aftsi)

	// Start listening for updates
	go txnManagerListen(aftsi)

	fmt.Printf("Starting server at %s.\n", time.Now().String())
	if err = server.Serve(lis); err != nil {
		log.Fatal("Could not start server on port %s: %v\n", TxnServerPort, err)
	}
}
