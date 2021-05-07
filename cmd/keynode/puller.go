package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	cmn "github.com/saurav-c/tasc/lib/common"
	kpb "github.com/saurav-c/tasc/proto/keynode"
	tpb "github.com/saurav-c/tasc/proto/tasc"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

func (keyNode *KeyNode) listener() {
	poller := zmq.NewPoller()
	zmqInfo := keyNode.ZmqInfo

	poller.Add(zmqInfo.readPuller, zmq.POLLIN)
	poller.Add(zmqInfo.validatePuller, zmq.POLLIN)
	poller.Add(zmqInfo.endTxnPuller, zmq.POLLIN)
	poller.Add(zmqInfo.clearPuller, zmq.POLLIN)

	for true {
		sockets, _ := poller.Poll(10 * time.Millisecond)

		for _, socket := range sockets {
			switch s := socket.Socket; s {
			case zmqInfo.readPuller:
				{
					req := &kpb.KeyNodeRequest{}
					data, _ := zmqInfo.readPuller.RecvBytes(zmq.DONTWAIT)
					proto.Unmarshal(data, req)
					log.Info("Received read request")
					go readHandler(keyNode, req)
				}
			case zmqInfo.validatePuller:
				{
					req := &kpb.ValidateRequest{}
					data, _ := zmqInfo.validatePuller.RecvBytes(zmq.DONTWAIT)
					proto.Unmarshal(data, req)
					log.Info("Received validate request")
					go validateHandler(keyNode, req)
				}
			case zmqInfo.endTxnPuller:
				{
					req := &kpb.EndRequest{}
					data, _ := zmqInfo.endTxnPuller.RecvBytes(zmq.DONTWAIT)
					proto.Unmarshal(data, req)
					go endTxnHandler(keyNode, req)
				}
			case zmqInfo.clearPuller:
				{
					zmqInfo.clearPuller.Recv(zmq.DONTWAIT)
					clearHandler(keyNode)
				}
			}
		}
	}
}

func readHandler(keyNode *KeyNode, req *kpb.KeyNodeRequest) {
	start := time.Now()
	keyVersion, _, coWrites, err := keyNode.readKey(req.Tid, req.Key,
		req.ReadSet, req.BeginTs, req.LowerBound)
	end := time.Now()

	go keyNode.Monitor.TrackStat(req.GetTid(), "[READ] Key Node Read", end.Sub(start))

	var resp *kpb.KeyNodeResponse
	if err != nil {
		resp = &kpb.KeyNodeResponse{
			Tid: req.Tid,
			Ok:  false,
		}
	} else {
		resp = &kpb.KeyNodeResponse{
			Tid:          req.Tid,
			KeyVersion:   keyVersion,
			CoWrittenSet: coWrites,
			Ok:           true,
		}
	}

	data, _ := proto.Marshal(resp)
	addr := fmt.Sprintf(cmn.PushTemplate, req.IpAddress, cmn.TxnReadPullPort)

	start = time.Now()
	keyNode.PusherCache.Lock(keyNode.ZmqInfo.context, addr)
	pusher := keyNode.PusherCache.GetSocket(addr)
	pusher.SendBytes(data, zmq.DONTWAIT)
	keyNode.PusherCache.Unlock(addr)
	end = time.Now()

	go keyNode.Monitor.TrackStat(req.Tid, "[READ] Key Node Read Pusher", end.Sub(start))
}

func validateHandler(keyNode *KeyNode, req *kpb.ValidateRequest) {
	start := time.Now()
	action := keyNode.validate(req.Tid, req.BeginTS, req.CommitTS, req.Keys)
	end := time.Now()

	go keyNode.Monitor.TrackStat(req.Tid, "[COMMIT] Key Node Validation", end.Sub(start))

	resp := &kpb.ValidateResponse{
		Tid:       req.Tid,
		IpAddress: keyNode.IpAddress,
		Action:    action,
	}
	data, _ := proto.Marshal(resp)

	addr := fmt.Sprintf(cmn.PushTemplate, req.IpAddress, cmn.TxnValidatePullPort)

	start = time.Now()
	keyNode.PusherCache.Lock(keyNode.ZmqInfo.context, addr)
	pusher := keyNode.PusherCache.GetSocket(addr)
	pusher.SendBytes(data, zmq.DONTWAIT)
	keyNode.PusherCache.Unlock(addr)
	end = time.Now()

	go keyNode.Monitor.TrackStat(req.Tid, "[COMMIT] Key Node Validation Pusher", end.Sub(start))
}

func endTxnHandler(keyNode *KeyNode, req *kpb.EndRequest) {
	start := time.Now()
	err := keyNode.endTransaction(req.Tid, req.Action, req.WriteSet)
	end := time.Now()

	go keyNode.Monitor.TrackStat(req.GetTid(), "[END] Key Node End", end.Sub(start))

	ok := true
	if err != nil {
		ok = false
	}

	resp := &kpb.EndResponse{
		Tid:       req.Tid,
		IpAddress: keyNode.IpAddress,
		Ok:        ok,
	}

	data, _ := proto.Marshal(resp)
	addr := fmt.Sprintf(cmn.PushTemplate, req.IpAddress, cmn.WorkerEndTxnPullPort)

	start = time.Now()
	keyNode.PusherCache.Lock(keyNode.ZmqInfo.context, addr)
	pusher := keyNode.PusherCache.GetSocket(addr)
	pusher.SendBytes(data, zmq.DONTWAIT)
	keyNode.PusherCache.Unlock(addr)
	end = time.Now()

	go keyNode.Monitor.TrackStat(req.Tid, "[END] Key Node End Pusher", end.Sub(start))
}

func clearHandler(keyNode *KeyNode) {
	keyNode.PendingVersionIndex.mutex.Lock()
	log.WithFields(log.Fields{
		"Struct": "Pending Locks",
		"Size": len(keyNode.PendingVersionIndex.locks),
	}).Debug("Cleared!")
	log.WithFields(log.Fields{
		"Struct": "Pending Index",
		"Size": len(keyNode.PendingVersionIndex.index),
	}).Debug("Cleared!")
	keyNode.PendingVersionIndex.locks = make(map[string]*sync.RWMutex)
	keyNode.PendingVersionIndex.index = make(map[string]*kpb.KeyVersionList)
	keyNode.PendingVersionIndex.mutex.Unlock()

	keyNode.CommittedVersionIndex.mutex.Lock()
	log.WithFields(log.Fields{
		"Struct": "Committed Locks",
		"Size": len(keyNode.CommittedVersionIndex.locks),
	}).Debug("Cleared!")
	log.WithFields(log.Fields{
		"Struct": "Committed Index",
		"Size": len(keyNode.CommittedVersionIndex.index),
	}).Debug("Cleared!")
	keyNode.CommittedVersionIndex.locks = make(map[string]*sync.RWMutex)
	keyNode.CommittedVersionIndex.index = make(map[string]*kpb.KeyVersionList)
	keyNode.CommittedVersionIndex.mutex.Unlock()

	keyNode.PendingTxnSet.mutex.Lock()
	log.WithFields(log.Fields{
		"Struct": "Pending Txn Set",
		"Size": len(keyNode.PendingTxnSet.txnSetMap),
	}).Debug("Cleared!")
	keyNode.PendingTxnSet.txnSetMap = make(map[string]*tpb.TransactionWriteSet)
	keyNode.PendingTxnSet.mutex.Unlock()

	keyNode.CommittedTxnSet.mutex.Lock()
	log.WithFields(log.Fields{
		"Struct": "Committed Txn Set",
		"Size": len(keyNode.CommittedTxnSet.txnSetMap),
	}).Debug("Cleared!")
	keyNode.CommittedTxnSet.txnSetMap = make(map[string]*tpb.TransactionWriteSet)
	keyNode.CommittedTxnSet.mutex.Unlock()

	log.Debug("Cleared all indexes and txn sets!")
}
