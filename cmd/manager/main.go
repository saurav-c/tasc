package main

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	uuid "github.com/nu7hatch/gouuid"
	zmq "github.com/pebbe/zmq4"
	cmn "github.com/saurav-c/tasc/lib/common"
	kpb "github.com/saurav-c/tasc/proto/keynode"
	rpb "github.com/saurav-c/tasc/proto/router"
	tpb "github.com/saurav-c/tasc/proto/tasc"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"strconv"
	"strings"
	"time"
)

func (t *TxnManager) StartTransaction(ctx context.Context, _ *empty.Empty) (*tpb.TransactionTag, error) {
	// Generate TID
	uid, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	tid := fmt.Sprintf(cmn.TidTemplate, t.Id, t.ThreadId, uid)

	defer t.Monitor.TrackFuncExecTime(tid, "Start Txn Time", time.Now())

	beginTs := time.Now().UnixNano()
	txnEntry := TransactionTableEntry{
		beginTs:      beginTs,
		readSet:      map[string]string{},
		coWrittenSet: map[string]string{},
		status:       Running,
		readChan:     make(chan *kpb.KeyNodeResponse),
		valChan:      make(chan *kpb.ValidateResponse),
		endTxnChan:   make(chan *kpb.EndResponse),
		rtrChan:      make(chan *rpb.RouterResponse),
	}
	bufferEntry := WriteBufferEntry{buffer: map[string][]byte{}}

	t.TransactionTable.mutex.Lock()
	t.TransactionTable.table[tid] = &txnEntry
	t.TransactionTable.mutex.Unlock()

	t.WriteBuffer.mutex.Lock()
	t.WriteBuffer.buffer[tid] = &bufferEntry
	t.WriteBuffer.mutex.Unlock()

	return &tpb.TransactionTag{
		Tid:    tid,
		Status: tpb.TascTransactionStatus_RUNNING,
		TxnManagerIP: t.PublicIP,
	}, nil
}

func (t *TxnManager) Read(ctx context.Context, requests *tpb.TascRequest) (*tpb.TascRequest, error) {
	tid := requests.Tid

	defer t.Monitor.TrackFuncExecTime(tid, "Read time", time.Now())

	t.TransactionTable.mutex.RLock()
	txnEntry := t.TransactionTable.table[tid]
	t.TransactionTable.mutex.RUnlock()

	t.WriteBuffer.mutex.RLock()
	bufferEntry := t.WriteBuffer.buffer[tid]
	t.WriteBuffer.mutex.RUnlock()

	resp := &tpb.TascRequest{Tid: tid}

	for _, request := range requests.Pairs {
		key := request.Key

		// Reading transaction's own write
		if val, ok := bufferEntry.buffer[key]; ok {
			resp.Pairs = append(resp.Pairs, &tpb.TascRequest_KeyPair{Key: key, Value: val})
			continue
		}

		// Reading transaction's previously read version
		if keyVersion, ok := txnEntry.readSet[key]; ok {
			val, err := t.StorageManager.Get(keyVersion)
			if err != nil {
				resp.Pairs = append(resp.Pairs, &tpb.TascRequest_KeyPair{Key: key, Value: val})
			}
			continue
		}

		go t.routerLookup(tid, []string{key})

		// Determine key version lower bound
		lowerBound := txnEntry.coWrittenSet[key]

		var readSet []string
		for _, keyVersion := range txnEntry.readSet {
			readSet = append(readSet, keyVersion)
		}
		readRequest := &kpb.KeyNodeRequest{
			Tid:        tid,
			Key:        key,
			ReadSet:    readSet,
			BeginTs:    txnEntry.beginTs,
			LowerBound: lowerBound,
			IpAddress:  t.IpAddress,
		}
		data, _ := proto.Marshal(readRequest)

		routingResp := <-txnEntry.rtrChan
		keyNodeIp, ok := routingResp.AddressMap[key]
		if !ok {
			log.Errorf("Unable to perform routing lookup for key %s", key)
			continue
		}
		addr := fmt.Sprintf(cmn.PushTemplate, keyNodeIp, cmn.KeyReadPullPort)

		start := time.Now()

		t.PusherCache.Lock(t.ZmqInfo.context, addr)
		pusher := t.PusherCache.GetSocket(addr)
		pusher.SendBytes(data, zmq.DONTWAIT)
		t.PusherCache.Unlock(addr)

		end := time.Now()
		go t.Monitor.TrackStat(tid, "Read Pusher Wait Time", end.Sub(start))

		readResponse := <-txnEntry.readChan

		if !readResponse.Ok {
			log.Errorf("Unable to read key %s", key)
			continue
		}

		// Update CoWrittenSet and Readset
		for _, keyVersion := range readResponse.CoWrittenSet {
			split := strings.Split(keyVersion, cmn.KeyDelimeter)
			k, v := split[0], split[1]
			if currVersion, ok := txnEntry.coWrittenSet[k]; !ok || cmn.CompareKeyVersion(v, currVersion) > 0 {
				txnEntry.coWrittenSet[k] = v
			}
		}
		txnEntry.readSet[key] = readResponse.KeyVersion

		resp.Pairs = append(resp.Pairs, &tpb.TascRequest_KeyPair{Key: key, Value: readResponse.Value})
	}
	return resp, nil
}

func (t *TxnManager) routerLookup(tid string, keys []string) {
	rtrReq := &rpb.RouterRequest{
		Tid:       tid,
		Keys:      keys,
		IpAddress: t.IpAddress,
	}
	data, _ := proto.Marshal(rtrReq)

	addr := fmt.Sprintf(cmn.PushTemplate, t.RouterIpAddress, cmn.RouterPullPort)
	start := time.Now()
	t.PusherCache.Lock(t.ZmqInfo.context, addr)
	validatePusher := t.PusherCache.GetSocket(addr)
	validatePusher.SendBytes(data, zmq.DONTWAIT)
	t.PusherCache.Unlock(addr)
	end := time.Now()

	go t.Monitor.TrackStat(tid, "Router pusher time", end.Sub(start))
}

func (t *TxnManager) Write(ctx context.Context, requests *tpb.TascRequest) (*tpb.TascRequest, error) {
	tid := requests.Tid

	defer t.Monitor.TrackFuncExecTime(tid, "Write time", time.Now())

	t.WriteBuffer.mutex.RLock()
	bufferEntry := t.WriteBuffer.buffer[tid]
	t.WriteBuffer.mutex.RUnlock()

	resp := &tpb.TascRequest{Tid: tid}
	for _, request := range requests.Pairs {
		bufferEntry.buffer[request.Key] = request.Value
		resp.Pairs = append(resp.Pairs, &tpb.TascRequest_KeyPair{Key: request.Key})
	}

	return resp, nil
}

func (t *TxnManager) CommitTransaction(ctx context.Context, tag *tpb.TransactionTag) (*tpb.TransactionTag, error) {
	tid := tag.Tid

	defer t.Monitor.TrackFuncExecTime(tid, "Commit time", time.Now())

	t.TransactionTable.mutex.RLock()
	txnEntry := t.TransactionTable.table[tid]
	t.TransactionTable.mutex.RUnlock()

	t.WriteBuffer.mutex.RLock()
	bufferEntry := t.WriteBuffer.buffer[tid]
	t.WriteBuffer.mutex.RUnlock()

	txnEntry.endTs = time.Now().UnixNano()

	if len(bufferEntry.buffer) == 0 {
		txnEntry.status = Complete
		return &tpb.TransactionTag{
			Tid:    tid,
			Status: tpb.TascTransactionStatus_COMMITTED,
		}, nil
	}

	writeSet := make([]string, 0, len(bufferEntry.buffer))
	writeVersionSet := make([]string, 0, len(bufferEntry.buffer))
	for k := range bufferEntry.buffer {
		writeSet = append(writeSet, k)
		writeVersionSet = append(writeSet, fmt.Sprintf(cmn.StorageKeyTemplate, k, cmn.Int64ToString(txnEntry.endTs), tid))
	}

	start := time.Now()
	t.routerLookup(tid, writeSet)
	routerResp := <-txnEntry.rtrChan
	end := time.Now()
	go t.Monitor.TrackStat(tid, "Router Lookup time", end.Sub(start))

	keyAddressMap := make(map[string][]string)
	phase1WaitMap := make(map[string]string)
	phase2WaitMap := make(map[string]string)
	for key, ipAddress := range routerResp.AddressMap {
		if _, ok := keyAddressMap[ipAddress]; !ok {
			keyAddressMap[ipAddress] = []string{}
			phase1WaitMap[ipAddress] = ""
			phase2WaitMap[ipAddress] = ""
		}
		keyAddressMap[ipAddress] = append(keyAddressMap[ipAddress], key)
	}

	start = time.Now()
	// Phase 1 of 2PC
	for keyNodeAddress, keys := range keyAddressMap {
		go t.validateTransaction(keyNodeAddress, keys, tid, txnEntry.beginTs, txnEntry.endTs)
	}

	// Preemptively start writing to storage
	storageChan := make(chan bool, 1)
	go t.writeToStorage(tid, txnEntry.endTs, bufferEntry, storageChan)

	// Wait for Phase 1 responses
	action := t.collectValidateResponses(tid, txnEntry.valChan, phase1WaitMap)
	end = time.Now()

	go t.Monitor.TrackStat(tid, "Phase 1 2PC Time", end.Sub(start))

	start = time.Now()
	for keyNodeAddress, _ := range keyAddressMap {
		if action == kpb.TransactionAction_COMMIT {
			txnEntry.status = Committing
			go t.endTransaction(keyNodeAddress, tid, action, writeVersionSet)
		} else {
			txnEntry.status = Aborting
			go t.endTransaction(keyNodeAddress, tid, action, nil)
		}
	}

	// Wait for Phase 2 responses
	ok := t.collectEndTxnResponses(tid, txnEntry.endTxnChan, phase2WaitMap)
	end = time.Now()

	go t.Monitor.TrackStat(tid, "Phase 2 2PC Time", end.Sub(start))

	// Rollback transaction
	if !ok && action == kpb.TransactionAction_COMMIT {
		action = kpb.TransactionAction_ABORT
		for keyNodeAddress, _ := range keyAddressMap {
			go t.endTransaction(keyNodeAddress, tid, action, nil)
		}
	}

	if action == kpb.TransactionAction_COMMIT {
		// Wait for storage write to finish
		<-storageChan
		txnEntry.status = Complete
		return &tpb.TransactionTag{
			Tid:    tid,
			Status: tpb.TascTransactionStatus_COMMITTED,
		}, nil
	} else {
		txnEntry.status = Complete
		return &tpb.TransactionTag{
			Tid:    tid,
			Status: tpb.TascTransactionStatus_ABORTED,
		}, nil
	}
}

func (t *TxnManager) AbortTransaction(ctx context.Context, tag *tpb.TransactionTag) (*tpb.TransactionTag, error) {
	tid := tag.Tid

	defer t.Monitor.TrackFuncExecTime(tid, "Abort time", time.Now())

	t.TransactionTable.mutex.RLock()
	txnEntry := t.TransactionTable.table[tid]
	t.TransactionTable.mutex.RUnlock()

	txnEntry.status = Complete
	return &tpb.TransactionTag{
		Tid:    tid,
		Status: tpb.TascTransactionStatus_ABORTED,
	}, nil
}

func (t *TxnManager) validateTransaction(keyNodeAddress string, keys []string, tid string, beginTs int64, endTs int64) {
	addr := fmt.Sprintf(cmn.PushTemplate, keyNodeAddress, cmn.KeyValidatePullPort)
	validateRequest := &kpb.ValidateRequest{
		Tid:       tid,
		BeginTS:   beginTs,
		CommitTS:  endTs,
		Keys:      keys,
		IpAddress: t.IpAddress,
	}
	data, _ := proto.Marshal(validateRequest)

	start := time.Now()
	t.PusherCache.Lock(t.ZmqInfo.context, addr)
	validatePusher := t.PusherCache.GetSocket(addr)
	validatePusher.SendBytes(data, zmq.DONTWAIT)
	t.PusherCache.Unlock(addr)
	end := time.Now()

	go t.Monitor.TrackStat(tid, "Validate pusher time", end.Sub(start))
}

func (t *TxnManager) collectValidateResponses(tid string, valRespChan chan *kpb.ValidateResponse,
	keyMap map[string]string) kpb.TransactionAction {
	keyNodeCount := len(keyMap)
	recvCount := 0
	timeout := time.NewTimer(1 * time.Second)
	for {
		select {
		case resp := <-valRespChan:
			if resp.Action == kpb.TransactionAction_ABORT {
				log.Debugf("Received abort from key node %s", resp.IpAddress)
				return kpb.TransactionAction_ABORT
			}
			recvCount += 1
			if recvCount == keyNodeCount {
				log.Debugf("Committing transaction %s", tid)
				return kpb.TransactionAction_COMMIT
			}
			delete(keyMap, resp.IpAddress)
		case <-timeout.C:
			var nodes []string
			for keyNode, _ := range keyMap {
				nodes = append(nodes, keyNode)
			}
			log.Errorf("Phase 1 Timeout waiting for: %v", nodes)
			return kpb.TransactionAction_ABORT
		}
	}
}

func (t *TxnManager) endTransaction(keyNodeAddress string, tid string, action kpb.TransactionAction,
	writeSet []string) {
	addr := fmt.Sprintf(cmn.PushTemplate, keyNodeAddress, cmn.KeyEndTxnPullPort)
	endReq := &kpb.EndRequest{
		Tid:       tid,
		Action:    action,
		WriteSet:  writeSet,
		IpAddress: t.IpAddress,
	}
	data, _ := proto.Marshal(endReq)

	start := time.Now()
	t.PusherCache.Lock(t.ZmqInfo.context, addr)
	endPusher := t.PusherCache.GetSocket(addr)
	endPusher.SendBytes(data, zmq.DONTWAIT)
	t.PusherCache.Unlock(addr)
	end := time.Now()

	go t.Monitor.TrackStat(tid, "End pusher time", end.Sub(start))
}

func (t *TxnManager) collectEndTxnResponses(tid string, endRespChan chan *kpb.EndResponse,
	keyMap map[string]string) bool {
	keyNodeCount := len(keyMap)
	recvCount := 0
	timeout := time.NewTimer(1 * time.Second)
	for {
		select {
		case resp := <-endRespChan:
			if !resp.Ok {
				return false
			}
			recvCount += 1
			if recvCount == keyNodeCount {
				return true
			}
			delete(keyMap, resp.IpAddress)
		case <-timeout.C:
			var nodes []string
			for keyNode, _ := range keyMap {
				nodes = append(nodes, keyNode)
			}
			log.Errorf("Phase 2 Timeout waiting for: %v", nodes)
			return false
		}
	}
}

func (t *TxnManager) writeToStorage(tid string, endTs int64, entry *WriteBufferEntry, writeChan chan bool) {
	go t.Monitor.TrackFuncExecTime(tid, "Write to Storage time", time.Now())

	dbKeys := make([]string, len(entry.buffer)+1)
	dbVals := make([][]byte, len(entry.buffer)+1)

	endTsString := cmn.Int64ToString(endTs)
	// Send writes & transaction set to storage
	i := 0
	for k, v := range entry.buffer {
		newKey := fmt.Sprintf(cmn.StorageKeyTemplate, k, endTsString, tid)
		dbKeys[i] = newKey
		dbVals[i] = v
		i++
	}
	txnWriteSet := &tpb.TransactionWriteSet{Keys: dbKeys}
	data, _ := proto.Marshal(txnWriteSet)
	dbKeys[i] = tid
	dbVals[i] = data

	t.StorageManager.MultiPut(dbKeys, dbVals)
	writeChan <- true
}

func (t *TxnManager) shutdown() {
	t.LogFile.Sync()
	t.LogFile.Close()
}

func main() {
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(cmn.TxnManagerServerPort))
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", cmn.TxnManagerServerPort, err)
	}

	server := grpc.NewServer()

	manager, err := NewTransactionManager(0)
	if err != nil {
		log.Fatal("Could not start server on port %s: %v\n", cmn.TxnManagerServerPort, err)
	}
	tpb.RegisterTascServer(server, manager)

	// Start listening for updates
	go manager.listener()

	// Cleanup
	defer manager.shutdown()

	// Send statistics to monitoring node
	go manager.Monitor.SendStats(1 * time.Second)

	log.Infof("Starting transaction manager at %s on thread %d", manager.IpAddress, manager.ThreadId)
	if err = server.Serve(lis); err != nil {
		log.Fatal("Could not start server on port %s: %v\n", cmn.TxnManagerServerPort, err)
	}
}
