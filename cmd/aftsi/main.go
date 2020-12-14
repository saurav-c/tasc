package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/google/uuid"
	zmq "github.com/pebbe/zmq4"
	router "github.com/saurav-c/aftsi/proto/routing"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	pb "github.com/saurav-c/aftsi/proto/aftsi"
	keyNode "github.com/saurav-c/aftsi/proto/keynode"

	"google.golang.org/grpc"
)

const (
	TxnServerPort = ":5000"
)

func _convertStringToBytes(stringSlice []string) []byte {
	stringByte := strings.Join(stringSlice, "\x20\x00")
	return []byte(stringByte)
}

func (s *AftSIServer) StartTransaction(ctx context.Context, emp *empty.Empty) (*pb.TransactionID, error) {
	start := time.Now()

	// Generate TID
	s.counterMutex.Lock()
	counter := s.counter
	s.counter += 1
	s.counterMutex.Unlock()
	tid := s.serverID + strconv.FormatUint(counter, 10)

	defer s.monitor.TrackFuncExecTime(tid,"Start Txn Time", start)

	s.CreateTransactionEntry(tid)
	return &pb.TransactionID{
		Tid: tid,
		E:   pb.TransactionError_SUCCESS,
	}, nil
}

func (s *AftSIServer) CreateTransactionEntry(tid string) {
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
}

func (s *AftSIServer) Read(ctx context.Context, readReq *pb.ReadRequest) (*pb.TransactionResponse, error) {
	// Parse read request fields
	tid := readReq.GetTid()
	key := readReq.GetKey()

	defer s.monitor.TrackFuncExecTime(tid,"Read time", time.Now())

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
		// Fetch Value From Storage (stored in val)
		val, err := s.StorageManager.Get(versionedKey)
		if err != nil {
			return &pb.TransactionResponse{
				Value: nil,
				E:     pb.TransactionError_FAILURE,
			}, nil
		}

		return &pb.TransactionResponse{
			Value: val,
			E:     pb.TransactionError_SUCCESS,
		}, nil
	}

	// Find lower bound for key version in order to maintain an atomic read set
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

	rSet := []string{}
	for _, v := range txnEntry.readSet {
		rSet = append(rSet, v)
	}

	cid := uuid.New().ID()
	rChan := make(chan *keyNode.KeyNodeResponse, 1)
	s.Responder.readMutex.Lock()
	s.Responder.readChannels[cid] = rChan
	s.Responder.readMutex.Unlock()
	// defer close(s.Responder.readChannels[cid])

	keyReq := &keyNode.KeyNodeRequest{
		Tid:        tid,
		Key:        key,
		ReadSet:    rSet,
		BeginTS:    txnEntry.beginTS,
		LowerBound: keyLowerBound,
		TxnMngrIP:  s.IPAddress,
		ChannelID:  cid,
	}

	data, _ := proto.Marshal(keyReq)
	addr := fmt.Sprintf(PushTemplate, keyIP, readPullPort)

	s.PusherCache.Lock(s.zmqInfo.context, addr)
	readPusher := s.PusherCache.GetSocket(addr)
	readPusher.SendBytes(data, zmq.DONTWAIT)
	s.PusherCache.Unlock(addr)

	readResponse := <-rChan

	if readResponse.GetError() != keyNode.KeyError_K_SUCCESS {
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
	for _, keyVersion := range coWrites {
		split := strings.Split(keyVersion, keyVersionDelim)
		k, v := split[0], split[1]
		if currentVersion, ok := txnEntry.coWrittenSet[k]; !ok || v > currentVersion {
			txnEntry.coWrittenSet[k] = v
		}
	}
	txnEntry.readSet[key] = versionedKey

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

	defer s.monitor.TrackFuncExecTime(tid, "Write time", time.Now())

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

	return &pb.TransactionResponse{
		E: pb.TransactionError_SUCCESS,
	}, nil
}

func (s *AftSIServer) CommitTransaction(ctx context.Context, req *pb.TransactionID) (*pb.TransactionResponse, error) {
	tid := req.GetTid()

	defer s.monitor.TrackFuncExecTime(tid, "Commit time", time.Now())

	cStart := time.Now()
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

	txnEntry.endTS = strconv.FormatInt(time.Now().UnixNano(), 10)

	writeSet := make([]string, 0, len(bufferEntry.buffer))
	writeVals := make([][]byte, 0, len(bufferEntry.buffer))
	for k, v := range bufferEntry.buffer {
		writeSet = append(writeSet, k)
		writeVals = append(writeVals, v)
	}
	cEnd := time.Now()
	go s.monitor.TrackStat(tid,"Commit Setup Time", cEnd.Sub(cStart))

	// Fetch KeyNode IP addresses
	rStart := time.Now()
	respRouter, err := s.keyRouterConn.MultipleLookUp(context.TODO(), &router.RouterReqMulti{Req: writeSet})
	rEnd := time.Now()
	go s.monitor.TrackStat(tid,"Router Lookup Time", rEnd.Sub(rStart))

	if err != nil {
		return &pb.TransactionResponse{E: pb.TransactionError_FAILURE}, err
	}
	multiResp := respRouter.GetIpMap()
	keyMap := make(map[string][]string)
	for ip, responseSet := range multiResp {
		keyMap[ip] = responseSet.GetResp()
	}

	validationChannel := make(chan bool, len(keyMap))

	start := time.Now()
	// Phase 1 of 2PC
	for ip, keys := range keyMap {
		go s.validateTransaction(ip, keys, txnEntry, tid, validationChannel)
	}

	// Preemptively start writing to storage
	storageChannel := make(chan bool, 1)
	go s.writeToStorage(tid, txnEntry.endTS, bufferEntry, storageChannel)

	// Wait for validation response from Key Node
	toCommit := false
	respCount := 0
	for ok := range validationChannel {
		if !ok {
			break
		}
		respCount += 1
		if respCount == len(keyMap) {
			toCommit = true
			break
		}
	}
	end := time.Now()
	go s.monitor.TrackStat(tid, "Phase 1 2PC Time", end.Sub(start))

	endChannel := make(chan bool, len(keyMap))

	start = time.Now()
	// Phase 2 of 2PC
	for ip, _ := range keyMap {
		go s.endTransaction(ip, tid, toCommit, endChannel, writeSet, writeVals)
	}

	// Wait for end transaction responses from Key Node
	respCount = 0
	for ok := range endChannel {
		if !ok {
			// TODO: Perform some sort of rollback?
		}
		respCount += 1
		if respCount == len(keyMap) {
			break
		}
	}
	end = time.Now()
	go s.monitor.TrackStat(tid, "Phase 2 2PC Time", end.Sub(start))

	if toCommit {
		// Wait for storage write to be done
		<-storageChannel
		sEnd := time.Now()
		go s.monitor.TrackStat(tid, "Extra Storage Wait Time", sEnd.Sub(end))
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

func (s *AftSIServer) validateTransaction(ipAddr string, keys []string,
	entry *TransactionEntry, tid string, responseChan chan bool) {
	addr := fmt.Sprintf(PushTemplate, ipAddr, validatePullPort)
	cid := uuid.New().ID()
	vChan := make(chan *keyNode.ValidateResponse, 1)
	s.Responder.valMutex.Lock()
	s.Responder.validateChannels[cid] = vChan
	s.Responder.valMutex.Unlock()
	// defer close(s.Responder.validateChannels[cid])

	vReq := &keyNode.ValidateRequest{
		Tid:       tid,
		BeginTS:   entry.beginTS,
		CommitTS:  entry.endTS,
		Keys:      keys,
		TxnMngrIP: s.IPAddress,
		ChannelID: cid,
	}
	data, _ := proto.Marshal(vReq)

	s.PusherCache.Lock(s.zmqInfo.context, addr)
	validatePusher := s.PusherCache.GetSocket(addr)

	validatePusher.SendBytes(data, zmq.DONTWAIT)
	s.PusherCache.Unlock(addr)

	// Timeout if no response heard
	select {
	case resp := <-vChan:
		responseChan <- resp.GetOk()
	case <-time.After(1 * time.Second):
		fmt.Printf("Timeout waiting for Key Node Phase 1: %s\n", ipAddr)
		responseChan <- false
	}
}

func (s *AftSIServer) writeToStorage(tid string, endTS string, entry *WriteBufferEntry, writeChan chan bool) {
	dbKeys := make([]string, len(entry.buffer)+1)
	dbVals := make([][]byte, len(entry.buffer)+1)

	// Send writes & transaction set to storage manager
	i := 0
	for k, v := range entry.buffer {
		newKey := k + keyVersionDelim + endTS + "-" + tid

		dbKeys[i] = newKey
		dbVals[i] = v
		i++
	}
	wSet := _convertStringToBytes(dbKeys)

	dbKeys[i] = tid
	dbVals[i] = wSet

	s.StorageManager.MultiPut(dbKeys, dbVals)

	writeChan <- true
}

func (s *AftSIServer) endTransaction(ipAddr string, tid string, toCommit bool, endChan chan bool,
	writeSet []string, writeVals [][]byte) {
	addr := fmt.Sprintf(PushTemplate, ipAddr, endTxnPort)
	cid := uuid.New().ID()
	eChan := make(chan *keyNode.FinishResponse, 1)
	s.Responder.endMutex.Lock()
	s.Responder.endTxnChannels[cid] = eChan
	s.Responder.endMutex.Unlock()
	// defer close(s.Responder.endTxnChannels[cid])

	// Create Finish Request
	var endReq *keyNode.FinishRequest
	if toCommit {
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

	s.PusherCache.Lock(s.zmqInfo.context, addr)
	endPusher := s.PusherCache.GetSocket(addr)

	endPusher.SendBytes(data, zmq.DONTWAIT)
	s.PusherCache.Unlock(addr)

	// Timeout if no response heard
	select {
	case resp := <-eChan:
		if resp.GetError() == keyNode.KeyError_K_FAILURE {
			endChan <- false
		} else {
			endChan <- true
		}
	case <-time.After(1 * time.Second):
		fmt.Printf("Timeout waiting for Key Node Phase 2: %s\n", ipAddr)
		endChan <- false
	}
}

func (s *AftSIServer) AbortTransaction(ctx context.Context, req *pb.TransactionID) (*pb.TransactionResponse, error) {
	tid := req.GetTid()

	defer s.monitor.TrackFuncExecTime(tid, "Abort time", time.Now())

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

func (s *AftSIServer) shutdown(debugMode bool) {
	if debugMode {
		s.logFile.Sync()
		s.logFile.Close()
	}
}

func main() {
	debug := flag.Bool("debug", false, "Debug Mode")
	flag.Parse()

	lis, err := net.Listen("tcp", TxnServerPort)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", TxnServerPort, err)
	}

	server := grpc.NewServer()

	aftsi, _, err := NewAftSIServer(*debug)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", TxnServerPort, err)
	}
	pb.RegisterAftSIServer(server, aftsi)

	// Start listening for updates
	go txnManagerListen(aftsi)

	// Cleanup
	defer aftsi.shutdown(*debug)

	// Send statistics to monitoring node
	go aftsi.monitor.SendStats(1 * time.Second)

	log.Infof("Starting transaction manager %s", aftsi.serverID)
	if err = server.Serve(lis); err != nil {
		log.Fatal("Could not start server on port %s: %v\n", TxnServerPort, err)
	}
}
