package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/google/uuid"
	zmq "github.com/pebbe/zmq4"
	"github.com/pkg/errors"
	router "github.com/saurav-c/aftsi/proto/routing/api"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
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
	allKeys := make([]string, 0)
	allValues := make([][]byte, 0)
	s.commitLock.RLock()
	for k, v := range s.commitBuffer {
		allKeys = append(allKeys, k)
		allValues = append(allValues, v)
	}
	s.commitLock.RUnlock()
	keysWritten, err := s.StorageManager.MultiPut(allKeys, allValues)
	if err != nil {
		s.commitLock.Lock()
		for _, key := range keysWritten {
			delete(s.commitBuffer, key)
		}
		s.commitLock.Unlock()
		return errors.New("Not all keys have been put")
	}
	s.commitLock.Lock()
	for _, key := range allKeys {
		delete(s.commitBuffer, key)
	}
	s.commitLock.Unlock()
	return nil
}

func (s *AftSIServer) StartTransaction(ctx context.Context, emp *empty.Empty) (*pb.TransactionID, error) {
	start := time.Now()
	// Generate TID
	s.counterMutex.Lock()
	tid := s.serverID + strconv.FormatUint(s.counter, 10)
	s.counter += 1
	s.counterMutex.Unlock()
	end := time.Now()
	fmt.Printf("Start API Call took: %f ms\n\n", 1000 * end.Sub(start).Seconds())

	s.CreateTransactionEntry(tid, "", 0)
	return &pb.TransactionID{
		Tid: tid,
		E:   pb.TransactionError_SUCCESS,
	}, nil
}

func (s *AftSIServer) Read(ctx context.Context, readReq *pb.ReadRequest) (*pb.TransactionResponse, error) {
	// Parse read request fields
	tid := readReq.GetTid()
	key := readReq.GetKey()

	s.TransactionMutex.RLock()
	txnEntry, ok := s.TransactionTable[tid]
	s.TransactionMutex.RUnlock()

	// No Transaction Found
	if !ok || txnEntry.status != TxnInProgress {
		return &pb.TransactionResponse{
			E: pb.TransactionError_FAILURE,
		}, nil
	}

	s.WriteBufferMutex.RLock()
	bufferEntry := s.WriteBuffer[tid]
	s.WriteBufferMutex.RUnlock()

	// Verify transaction status
	beginTS := txnEntry.beginTS

	// Reading from the Write Buffer
	if val, ok := bufferEntry.buffer[key]; ok {
		return &pb.TransactionResponse{
			Value: val,
			E:     pb.TransactionError_SUCCESS,
		}, nil
	}

	// Reading from the ReadSet
	versionedKey, ok := txnEntry.readSet[key]
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
	coWrittenSet := txnEntry.coWrittenSet
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
	for _, v := range txnEntry.readSet {
		rSet = append(rSet, v)
	}

	cid := uuid.New().ID()
	rChan := make(chan *keyNode.KeyResponse, 1)
	s.Responder.readMutex.Lock()
	s.Responder.readChannels[cid] = rChan
	s.Responder.readMutex.Unlock()
	// defer close(s.Responder.readChannels[cid])

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

	readResponse := <-rChan

	if readResponse.GetError() != keyNode.KeyError_SUCCESS {
		return &pb.TransactionResponse{
			E: pb.TransactionError_FAILURE,
		}, nil
	}
	versionedKey = readResponse.GetKeyVersion()
	fmt.Printf("Reading key version: %s\n", versionedKey)
	val := readResponse.GetValue()
	coWrites := readResponse.GetCoWrittenSet()

	if versionedKey == "default" {
		return &pb.TransactionResponse{
			Value: val,
			E:     pb.TransactionError_SUCCESS,
		}, nil
	}

	// Update CoWrittenSet and Readset
	for _, keyVersion := range coWrites {
		split := strings.Split(keyVersion, keyVersionDelim)
		k, v := split[0], split[1]
		if currentVersion, ok := txnEntry.coWrittenSet[k]; !ok || v > currentVersion {
			txnEntry.coWrittenSet[k] = v
		}
	}
	txnEntry.readSet[key] = versionedKey

	// Cache value
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

	s.TransactionMutex.RLock()
	txnEntry, ok := s.TransactionTable[tid]
	s.TransactionMutex.RUnlock()

	// No Transaction Found
	if !ok || txnEntry.status != TxnInProgress {
		return &pb.TransactionResponse{
			E: pb.TransactionError_FAILURE,
		}, nil
	}

	s.WriteBufferMutex.RLock()
	bufferEntry := s.WriteBuffer[tid]
	s.WriteBufferMutex.RUnlock()

	bufferEntry.buffer[key] = val

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

	s.TransactionMutex.RLock()
	txnEntry, ok := s.TransactionTable[tid]
	s.TransactionMutex.RUnlock()

	// No Transaction Found
	if !ok || txnEntry.status != TxnInProgress {
		return &pb.TransactionResponse{
			E: pb.TransactionError_FAILURE,
		}, nil
	}

	s.WriteBufferMutex.RLock()
	bufferEntry := s.WriteBuffer[tid]
	s.WriteBufferMutex.RUnlock()

	if len(bufferEntry.buffer) == 0 {
		txnEntry.status = TxnCommitted
		return &pb.TransactionResponse{
			E: pb.TransactionError_SUCCESS,
		}, nil
	}

	// Do a "routing" request for keys in writeSet
	writeSet := make([]string, 0, len(bufferEntry.buffer))
	writeVals := make([][]byte, 0, len(bufferEntry.buffer))
	for k, v := range bufferEntry.buffer {
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

	txnEntry.endTS = strconv.FormatInt(time.Now().UnixNano(), 10)

	// TODO: Do 2PC for KeyNodes
	for ip, keys := range keyMap {
		go func(ip string, keys []string){
			addr := fmt.Sprintf(PushTemplate, ip, validatePullPort)
			cid := uuid.New().ID()
			vChan := make(chan *keyNode.ValidateResponse, 1)
			s.Responder.valMutex.Lock()
			s.Responder.validateChannels[cid] = vChan
			s.Responder.valMutex.Unlock()
			// defer close(s.Responder.validateChannels[cid])

			vReq := &keyNode.ValidateRequest{
				Tid:       tid,
				BeginTS:   txnEntry.beginTS,
				CommitTS:  txnEntry.endTS,
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
			case resp := <-vChan:
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

	storageChannel := make(chan int, 1)
	if commit {
		go func() {
			startWrite := time.Now()
			dbKeys := make([]string, len(bufferEntry.buffer)+1)
			dbVals := make([][]byte, len(bufferEntry.buffer)+1)
			// Send writes & transaction set to storage manager
			i := 0
			for k, v := range bufferEntry.buffer {
				newKey := k + keyVersionDelim + txnEntry.endTS + "-" + tid
				fmt.Printf("Wrote key %s\n", newKey)

				dbKeys[i] = newKey
				if s.batchMode {
					s._addToBuffer(newKey, v)
				} else {
					dbVals[i] = v
				}
				i++
			}
			wSet := _convertStringToBytes(dbKeys)
			if s.batchMode {
				s._addToBuffer(tid, wSet)
			} else {
				dbKeys[i] = tid
				dbVals[i] = wSet
				s.StorageManager.MultiPut(dbKeys, dbVals)
			}
			endWrite := time.Now()
			fmt.Printf("Write to storage time: %f ms\n", 1000 * endWrite.Sub(startWrite).Seconds())
			storageChannel <- 1
		}()
	} else {
		storageChannel <- 1
	}

	finishTxnChannel := make(chan bool, len(keyMap))
	// Phase 2 of 2PC to Send ABORT/COMMIT to all Key Nodes
	for ip, _ := range keyMap {
		go func(ip string) {
			addr := fmt.Sprintf(PushTemplate, ip, endTxnPort)
			cid := uuid.New().ID()
			eChan := make(chan *keyNode.FinishResponse, 1)
			s.Responder.endMutex.Lock()
			s.Responder.endTxnChannels[cid] = eChan
			s.Responder.endMutex.Unlock()
			// defer close(s.Responder.endTxnChannels[cid])

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
			case resp := <-eChan:
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
			break
		}
	}

	end := time.Now()
	fmt.Printf("Txn Manager Commit API Time: %f ms\n\n", 1000 * end.Sub(start).Seconds())

	// Wait for storage write to be done
	<- storageChannel

	if commit {
		txnEntry.status = TxnCommitted
		return &pb.TransactionResponse{
			E: pb.TransactionError_SUCCESS,
		}, nil
	} else {
		txnEntry.status = TxnAborted
		return &pb.TransactionResponse{
			E: pb.TransactionError_FAILURE,
		}, nil
	}
}

func (s *AftSIServer) AbortTransaction(ctx context.Context, req *pb.TransactionID) (*pb.TransactionResponse, error) {
	tid := req.GetTid()

	s.TransactionMutex.RLock()
	txnEntry, ok := s.TransactionTable[tid]
	s.TransactionMutex.RUnlock()

	// No Transaction Found
	if !ok || txnEntry.status != TxnInProgress {
		return &pb.TransactionResponse{
			E: pb.TransactionError_FAILURE,
		}, nil
	}

	txnEntry.status = TxnAborted
	return &pb.TransactionResponse{
		E: pb.TransactionError_SUCCESS,
	}, nil
}

func (s *AftSIServer) CreateTransactionEntry(tid string, txnManagerIP string, channelID uint32) () {
	s.TransactionMutex.Lock()
	s.TransactionTable[tid] = &TransactionEntry{}
	s.TransactionMutex.Unlock()

	s.WriteBufferMutex.Lock()
	s.WriteBuffer[tid] = &WriteBufferEntry{buffer: make(map[string][]byte)}
	s.WriteBufferMutex.Unlock()

	entry := &TransactionEntry{
		beginTS:      strconv.FormatInt(time.Now().UnixNano(), 10),
		readSet:      make(map[string]string),
		coWrittenSet: make(map[string]string),
		status:       TxnInProgress,
	}

	s.TransactionMutex.Lock()
	s.TransactionTable[tid] = entry
	s.TransactionMutex.Unlock()

	if txnManagerIP == "" {
		return
	}

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

	personalIP := flag.String("addr", "", "Personal IP")
	keyRouter := flag.String("keyRtr", "", "Key Router IP")
	storage := flag.String("storage", "dynamo", "Storage Engine")

	batchMode := flag.Bool("batch", false, "Whether to do batch updates or not")

	flag.Parse()

	server := grpc.NewServer()

	fmt.Printf("Batch Mode: %t\n", *batchMode)


	aftsi, _, err := NewAftSIServer(*personalIP, *keyRouter, *storage, *batchMode)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", TxnServerPort, err)
	}
	pb.RegisterAftSIServer(server, aftsi)

	// Start listening for updates
	go txnManagerListen(aftsi)
	if *batchMode {
		go flusher(aftsi)
	}

	fmt.Printf("Starting server at %s.\n", time.Now().String())
	if err = server.Serve(lis); err != nil {
		log.Fatal("Could not start server on port %s: %v\n", TxnServerPort, err)
	}
}
