package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	zmq "github.com/pebbe/zmq4"
	"github.com/pkg/errors"
	router "github.com/saurav-c/aftsi/proto/routing/api"
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

func _convertStringToBytes(stringSlice []string) ([]byte) {
	stringByte := strings.Join(stringSlice, "\x20\x00")
	return []byte(stringByte)
}

func (s *AftSIServer) _addToBuffer(key string, value []byte) {
	s.commitLock.Lock()
	s.commitBuffer[key] = value
	s.commitLock.Unlock()
}

func (s *AftSIServer) _flushBuffer() error {
	s.commitLock.Lock()
	copyCommitBuffer := s.commitBuffer
	s.commitLock.Unlock()
	allKeys := make([]string, 0)
	allValues := make([][]byte, 0)
	for k, v := range copyCommitBuffer {
		allKeys = append(allKeys, k)
		allValues = append(allValues, v)
	}
	keysWritten, err := s.StorageManager.MultiPut(allKeys, allValues)
	if err != nil {
		for _, key := range keysWritten {
			delete(s.commitBuffer, key)
		}
		return errors.New("Not all keys have been put")
	}
	for key := range copyCommitBuffer {
		delete(s.commitBuffer, key)
	}
	return nil
}

func (s *AftSIServer) StartTransaction(ctx context.Context, emp *empty.Empty) (*pb.TransactionID, error) {
	start := time.Now()
	// Generate TID
	s.counterMutex.Lock()
	tid := s.serverID + strconv.FormatUint(s.counter, 10)
	s.counter += 1
	s.counterMutex.Unlock()

	startRouter := time.Now()
	// Ask router for the IP address of master for this TID
	respRouter, err := s.txnRouterConn.LookUp(context.TODO(), &router.RouterReq{Req: tid})
	if err != nil {
		return nil, errors.New("Router Lookup Failed.")
	}
	endRouter := time.Now()
	fmt.Printf("Router lookup took: %f ms\n", 1000 * endRouter.Sub(startRouter).Seconds())

	txnManagerIP := respRouter.GetIp()

	// Create Channel to listen for response
	cid := uuid.New().ID()
	s.Responder.createTxnChannels[cid] = make(chan *pb.CreateTxnEntryResp, 1)
	defer close(s.Responder.createTxnChannels[cid])

	txnEntryReq := &pb.CreateTxnEntry{
		Tid:          tid,
		TxnManagerIP: s.IPAddress,
		ChannelID:    cid,
	}
	data, _ := proto.Marshal(txnEntryReq)

	addr := fmt.Sprintf(PushTemplate, txnManagerIP, createTxnPortReq)
	s.PusherCache.lock(s.zmqInfo.context, addr)
	pusher := s.PusherCache.getSocket(addr)

	startCreate := time.Now()
	pusher.SendBytes(data, zmq.DONTWAIT)
	s.PusherCache.unlock(addr)

	// Wait for response
	resp := <-s.Responder.createTxnChannels[cid]
	endCreate := time.Now()
	fmt.Printf("Create Txn took: %f ms\n", 1000 * endCreate.Sub(startCreate).Seconds())

	end := time.Now()
	fmt.Printf("Start API Call tokk: %f ms\n\n", 1000 * end.Sub(start).Seconds())

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
	// TODO: Go thru each cowritten set and check for this key, find max version and send it to KN
	s.TransactionTableLock[tid].RLock()
	coWrittenSet := s.TransactionTable[tid].coWrittenSet
	s.TransactionTableLock[tid].RUnlock()
	keyLowerBound := ""
	if version, ok := coWrittenSet[key]; ok {
		keyLowerBound = version
	}

	// Fetch Correct Version of Key from KeyNode and read from Storage
	// Get Key Node for this key
	resp, err := s.keyRouterConn.LookUp(context.TODO(), &router.RouterReq{Req: key})
	if err != nil {
		return &pb.TransactionResponse{E: pb.TransactionError_FAILURE}, err
	}

	keyIP := resp.GetIp()

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

	// TODO: Send update to replicas

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

	// Fetch KeyNode IP addresses
	respRouter, err := s.keyRouterConn.MultipleLookUp(context.TODO(), &router.RouterReqMulti{Req: writeSet})
	if err != nil {
		return &pb.TransactionResponse{E: pb.TransactionError_FAILURE}, err
	}
	multiResp := respRouter.GetIpMap()
	keyMap := make(map[string][]string)
	for ip, responseSet := range multiResp {
		keyMap[ip] = responseSet.GetResp()
	}

	validationChannel := make(chan bool, len(keyMap))

	commitTS := strconv.FormatInt(time.Now().UnixNano(), 10)
	s.TransactionTable[tid].endTS = commitTS

	// TODO: Do 2PC for KeyNodes
	for ip, keys := range keyMap {
		go func(ip string, keys []string){
			addr := fmt.Sprintf(PushTemplate, ip, validatePullPort)
			cid := uuid.New().ID()
			s.Responder.validateChannels[cid] = make(chan *keyNode.ValidateResponse, 1)
			defer close(s.Responder.validateChannels[cid])

			vReq := &keyNode.ValidateRequest{
				Tid:       tid,
				BeginTS:   s.TransactionTable[tid].beginTS,
				CommitTS:  commitTS,
				Keys:      keys,
				TxnMngrIP: s.IPAddress,
				ChannelID: cid,
			}
			data, _ := proto.Marshal(vReq)

			s.PusherCache.lock(s.zmqInfo.context, addr)
			validatePusher := s.PusherCache.getSocket(addr)

			startVal := time.Now()

			validatePusher.SendBytes(data, zmq.DONTWAIT)
			s.PusherCache.unlock(addr)

			// Timeout if no response heard
			select {
			case resp := <-s.Responder.validateChannels[cid]:
				validationChannel <- resp.GetOk()
			case <-time.After(1 * time.Second):
				fmt.Printf("Timeout waiting for Key Node Phase 1: %s\n", ip)
				validationChannel <- false
			}
			endVal := time.Now()
			fmt.Printf("Validation time: %f ms\n", 1000 * endVal.Sub(startVal).Seconds())
		}(ip, keys)
	}

	// Wait for responses from Key Node
	commit := false
	respCount := 0
	for ok := range validationChannel {
		if !ok {
			break
		}
		respCount += 1
		if respCount == len(keyMap) {
			commit = true
			break
		}
	}

	startWrite := time.Now()
	if commit {
		writeSet := make([]string, 0)
		// Send writes & transaction set to storage manager
		for k, v := range s.WriteBuffer[tid] {
			newKey := k+keyVersionDelim+commitTS+"-"+tid
			s._addToBuffer(newKey, v)
			writeSet = append(writeSet, newKey)
		}
		s._addToBuffer(tid, _convertStringToBytes(writeSet))
	}
	endWrite := time.Now()
	fmt.Printf("Write to storage time: %f ms\n", 1000 * endWrite.Sub(startWrite).Seconds())

	finishTxnChannel := make(chan bool, len(keyMap))
	// Phase 2 of 2PC to Send ABORT/COMMIT to all Key Nodes
	for ip, _ := range keyMap {
		go func(ip string) {
			addr := fmt.Sprintf(PushTemplate, ip, endTxnPort)
			cid := uuid.New().ID()
			s.Responder.endTxnChannels[cid] = make(chan *keyNode.FinishResponse, 1)
			defer close(s.Responder.endTxnChannels[cid])

			// Create Finish Request
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

			data, _ := proto.Marshal(endReq)

			s.PusherCache.lock(s.zmqInfo.context, addr)
			endPusher := s.PusherCache.getSocket(addr)

			startEnd := time.Now()

			endPusher.SendBytes(data, zmq.DONTWAIT)
			s.PusherCache.unlock(addr)

			// Timeout if no response heard
			select {
			case resp := <-s.Responder.endTxnChannels[cid]:
				if resp.GetError() == keyNode.KeyError_FAILURE {
					finishTxnChannel <- false
				} else {
					finishTxnChannel <- true
				}
			case <-time.After(1 * time.Second):
				fmt.Printf("Timeout waiting for Key Node Phase 2: %s\n", ip)
				finishTxnChannel <- false
			}
			endEnd := time.Now()
			fmt.Printf("End Txn time: %f ms\n", 1000 * endEnd.Sub(startEnd).Seconds())
		}(ip)
	}

	// Wait for responses from Key Node
	respCount = 0
	for ok := range finishTxnChannel {
		if !ok {
			// TODO: Perform some sort of rollback?
		}
		respCount += 1
		if respCount == len(keyMap) {
			commit = true
			break
		}
	}

	end := time.Now()
	fmt.Printf("Txn Manager Commit API Time: %f ms\n\n", 1000 * end.Sub(start).Seconds())

	if commit {
		s.TransactionTable[tid].status = TxnCommitted
		return &pb.TransactionResponse{
			E: pb.TransactionError_SUCCESS,
		}, nil
	} else {
		s.TransactionTable[tid].status = TxnAborted
		return &pb.TransactionResponse{
			E: pb.TransactionError_FAILURE,
		}, nil
	}
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

func (s *AftSIServer) CreateTransactionEntry(tid string, txnManagerIP string, channelID uint32) () {

	s.TransactionTable[tid] = &TransactionEntry{
		beginTS:      strconv.FormatInt(time.Now().UnixNano(), 10),
		readSet:      make(map[string]string),
		coWrittenSet: make(map[string]string),
		status:       TxnInProgress,
	}
	s.WriteBuffer[tid] = make(map[string][]byte)

	s.TransactionTableLock[tid] = &sync.RWMutex{}
	s.WriteBufferLock[tid] = &sync.RWMutex{}

	resp := &pb.CreateTxnEntryResp{
		E:         pb.TransactionError_SUCCESS,
		ChannelID: channelID,
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
	txnRouter := os.Args[2]
	keyRouter := os.Args[3]
	storage := os.Args[4]

	server := grpc.NewServer()


	aftsi, _, err := NewAftSIServer(personalIP, txnRouter, keyRouter, storage, true)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", TxnServerPort, err)
	}
	pb.RegisterAftSIServer(server, aftsi)

	// Start listening for updates
	go txnManagerListen(aftsi)
	go flusher(aftsi)

	fmt.Printf("Starting server at %s.\n", time.Now().String())
	if err = server.Serve(lis); err != nil {
		log.Fatal("Could not start server on port %s: %v\n", TxnServerPort, err)
	}
}
