package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	zmq "github.com/pebbe/zmq4"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	pb "github.com/saurav-c/aftsi/proto/aftsi/api"
	keyNode "github.com/saurav-c/aftsi/proto/keynode/api"

	"google.golang.org/grpc"
)

const (
	TxnServerPort = ":5000"
)

func _HelperGetPort() (port int64, err error) {
	conn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	addr := conn.Addr().String()
	_, portString, err := net.SplitHostPort(addr)
	if err != nil {
		return -1, err
	}

	port_og, err := strconv.Atoi(portString)
	if err != nil {
		return -1, err
	}

	port = int64(port_og)
	return port, nil
}

func (s *AftSIServer) StartTransaction(ctx context.Context, emp *empty.Empty) (*pb.TransactionID, error) {
	// Generate TID
	s.counterMutex.Lock()
	tid := s.serverID + strconv.FormatUint(s.counter, 10)
	s.counter += 1
	s.counterMutex.Unlock()

	// Ask router for the IP address of master for this TID
	//txnManagerIP, err := pingTxnRouter(&s.zmqInfo, tid)
	//if err != nil {
	//	return &pb.TransactionID{
	//		Tid: "",
	//		E:   pb.TransactionError_FAILURE,
	//	}, nil
	//}

	txnManagerIP := s.IPAddress

	// Create Channel to listen for response
	cid := uuid.New().ID()
	s.Responder.createTxnChannels[cid] = make(chan *pb.CreateTxnEntryResp, 1)
	defer close(s.Responder.readChannels[cid])

	txnEntryReq := &pb.CreateTxnEntry{
		Tid:          tid,
		TxnManagerIP: s.IPAddress,
		ChannelID:    cid,
	}
	data, _ := proto.Marshal(txnEntryReq)

	addr := fmt.Sprintf(PushTemplate, txnManagerIP, createTxnPortReq)
	s.PusherCache.lock(s.zmqInfo.context, addr)
	pusher := s.PusherCache.getSocket(addr)
	pusher.SendBytes(data, zmq.DONTWAIT)
	s.PusherCache.unlock(addr)

	// Wait for response
	resp := <-s.Responder.createTxnChannels[cid]

	if resp.GetE() != pb.TransactionError_SUCCESS {
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
	var beginTS string
	s.TransactionTableLock[tid].RLock()
	if entry, ok := s.TransactionTable[tid]; !ok || entry.status != TxnInProgress {
		s.TransactionTableLock[tid].RUnlock()
		return &pb.TransactionResponse{
			E: pb.TransactionError_FAILURE,
		}, nil
	}
	beginTS = s.TransactionTable[tid].beginTS
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
		val, err := s.StorageManager.Get(versionedKey)
		if err != nil {
			return &pb.TransactionResponse{
				Value: nil,
				E:     pb.TransactionError_FAILURE,
			}, nil
		}

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
	//keyIP, err := pingKeyRouter(&s.zmqInfo, key)
	//if err != nil {
	//	return &pb.TransactionResponse{
	//		Value: nil,
	//		E:     pb.TransactionError_FAILURE,
	//	}, nil
	//}

	keyIP := s.KeyNodeIP

	// Use ZMQ to make a read request to the Key Node at keyIP

	rSet := []string{}
	for _, v := range readSet {
		rSet = append(rSet, v)
	}

	cid := uuid.New().ID()
	s.Responder.readChannels[cid] = make(chan *keyNode.KeyResponse, 1)
	defer close(s.Responder.readChannels[cid])

	keyReq := &keyNode.KeyRequest{
		Tid:        tid,
		Key:        key,
		ReadSet:    rSet,
		BeginTS:    beginTS,
		LowerBound: keyLowerBound,
		TxnMngrIP:  s.IPAddress,
		ChannelID:  cid,
	}

	data, _ := proto.Marshal(keyReq)
	addr := fmt.Sprintf(PushTemplate, keyIP, readPullPort)

	s.PusherCache.lock(s.zmqInfo.context, addr)
	readPusher := s.PusherCache.getSocket(addr)
	readPusher.SendBytes(data, zmq.DONTWAIT)
	s.PusherCache.unlock(addr)

	readResponse := <-s.Responder.readChannels[cid]

	if readResponse.GetError() != keyNode.KeyError_SUCCESS {
		return &pb.TransactionResponse{
			E: pb.TransactionError_FAILURE,
		}, nil
	}
	versionedKey = readResponse.GetKeyVersion()
	val := readResponse.GetValue()
	coWrites := readResponse.GetCoWrittenSet()

	if versionedKey == "default" {
		return &pb.TransactionResponse{
			Value: val,
			E:     pb.TransactionError_SUCCESS,
		}, nil
	}

	// Update CoWrittenSet and Readset
	s.TransactionTableLock[tid].Lock()
	for _, keyVersion := range coWrites {
		split := strings.Split(keyVersion, keyVersionDelim)
		k, v := split[0], split[1]
		if currentVersion, ok := s.TransactionTable[tid].coWrittenSet[k]; !ok || v > currentVersion {
			s.TransactionTable[tid].coWrittenSet[k] = v
		}
	}
	s.TransactionTable[tid].readSet[key] = versionedKey
	s.TransactionTableLock[tid].Unlock()

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
	start := time.Now()

	// Parse request TID
	tid := req.GetTid()

	// Lock the TxnEntry and Write Buffer
	s.TransactionTableLock[tid].Lock()
	s.WriteBufferLock[tid].Lock()
	defer s.TransactionTableLock[tid].Unlock()
	defer s.WriteBufferLock[tid].Unlock()

	// Do a "routing" request for keys in writeSet
	buffer := s.WriteBuffer[tid]
	writeSet := make([]string, 0, len(buffer))
	writeVals := make([][]byte, 0, len(buffer))
	for k, v := range buffer {
		writeSet = append(writeSet, k)
		writeVals = append(writeVals, v)
	}

	// Get Keynode ip
	ip := s.KeyNodeIP
	addr := fmt.Sprintf(PushTemplate, ip, validatePullPort)

	commitTS := strconv.FormatInt(time.Now().UnixNano(), 10)
	s.TransactionTable[tid].endTS = commitTS

	cid := uuid.New().ID()
	s.Responder.validateChannels[cid] = make(chan *keyNode.ValidateResponse, 1)
	defer close(s.Responder.validateChannels[cid])

	vReq := &keyNode.ValidateRequest{
		Tid:       tid,
		BeginTS:   s.TransactionTable[tid].beginTS,
		CommitTS:  commitTS,
		Keys:      writeSet,
		TxnMngrIP: s.IPAddress,
		ChannelID: cid,
	}

	data, _ := proto.Marshal(vReq)

	s.PusherCache.lock(s.zmqInfo.context, addr)
	validatePusher := s.PusherCache.getSocket(addr)

	startVal := time.Now()

	validatePusher.SendBytes(data, zmq.DONTWAIT)
	s.PusherCache.unlock(addr)

	resp := <-s.Responder.validateChannels[cid]

	endVal := time.Now()
	fmt.Printf("Validation time: %f\n", endVal.Sub(startVal).Seconds())

	// Check that it is ok or not
	startWrite := time.Now()
	commit := resp.GetOk()
	if commit {
		// Send writes & transaction set to storage manager
		for k, v := range s.WriteBuffer[tid] {
			s.StorageManager.Put(k+keyVersionDelim+commitTS+"-"+tid, v)
		}
	}
	endWrite := time.Now()
	fmt.Printf("Write to storage time: %f\n", endWrite.Sub(startWrite).Seconds())

	cid = uuid.New().ID()
	s.Responder.endTxnChannels[cid] = make(chan *keyNode.FinishResponse, 1)
	defer close(s.Responder.endTxnChannels[cid])

	var endReq *keyNode.FinishRequest
	if commit {
		endReq = &keyNode.FinishRequest{
			Tid:         tid,
			S:           keyNode.TransactionAction_COMMIT,
			WriteSet:    writeSet,
			WriteBuffer: writeVals,
			TxnMngrIP:   s.IPAddress,
			ChannelID:   cid,
		}
	} else {
		endReq = &keyNode.FinishRequest{
			Tid:       tid,
			S:         keyNode.TransactionAction_ABORT,
			TxnMngrIP: s.IPAddress,
			ChannelID: cid,
		}
	}

	data, _ = proto.Marshal(endReq)

	addr = fmt.Sprintf(PushTemplate, ip, endTxnPort)
	s.PusherCache.lock(s.zmqInfo.context, addr)
	endPusher := s.PusherCache.getSocket(addr)

	startEnd := time.Now()

	endPusher.SendBytes(data, zmq.DONTWAIT)
	s.PusherCache.unlock(addr)

	// Wait for Ack
	endResp := <-s.Responder.endTxnChannels[cid]

	endEnd := time.Now()
	fmt.Printf("End Txn time: %f\n", endEnd.Sub(startEnd).Seconds())

	// Change commmit status
	if endResp.GetError() == keyNode.KeyError_FAILURE {
		return &pb.TransactionResponse{
			E: pb.TransactionError_FAILURE,
		}, nil
	}

	if commit {
		s.TransactionTable[tid].status = TxnCommitted
	} else {
		s.TransactionTable[tid].status = TxnAborted
	}

	end := time.Now()
	fmt.Printf("Txn Manager Commit API Time: %f\n", end.Sub(start).Seconds())
	// Respond to client
	return &pb.TransactionResponse{
		E: pb.TransactionError_SUCCESS,
	}, nil

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
		beginTS:      strconv.FormatInt(time.Now().UnixNano(), 10),
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

	addr := fmt.Sprintf(PushTemplate, txnManagerIP, createTxnPortResp)
	s.PusherCache.lock(s.zmqInfo.context, addr)
	pusher := s.PusherCache.getSocket(addr)
	pusher.SendBytes(data, zmq.DONTWAIT)
	s.PusherCache.unlock(addr)
}

func main() {
	lis, err := net.Listen("tcp", TxnServerPort)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", TxnServerPort, err)
	}

	personalIP := os.Args[1]
	keyNodeIP := os.Args[2]
	storage := os.Args[3]

	server := grpc.NewServer()
	// TODO: Lookup router IP Address
	txnRouter := ""
	keyRouter := ""

	aftsi, _, err := NewAftSIServer(personalIP, txnRouter, keyRouter, keyNodeIP, storage, true)
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
