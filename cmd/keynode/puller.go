package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	cmn "github.com/saurav-c/tasc/lib/common"
	kpb "github.com/saurav-c/tasc/proto/keynode"
	log "github.com/sirupsen/logrus"
	"time"
)

func (keyNode *KeyNode) listener() {
	poller := zmq.NewPoller()
	zmqInfo := keyNode.ZmqInfo

	poller.Add(zmqInfo.readPuller, zmq.POLLIN)
	poller.Add(zmqInfo.validatePuller, zmq.POLLIN)
	poller.Add(zmqInfo.endTxnPuller, zmq.POLLIN)

	for true {
		sockets, _ := poller.Poll(10 * time.Millisecond)

		for _, socket := range sockets {
			switch s := socket.Socket; s {
			case zmqInfo.readPuller:
				{
					req := &kpb.KeyNodeRequest{}
					data, _ := zmqInfo.readPuller.RecvBytes(zmq.DONTWAIT)
					proto.Unmarshal(data, req)
					go readHandler(keyNode, req)
				}
			case zmqInfo.validatePuller:
				{
					req := &kpb.ValidateRequest{}
					data, _ := zmqInfo.validatePuller.RecvBytes(zmq.DONTWAIT)
					proto.Unmarshal(data, req)
					go validateHandler(keyNode, req)
				}
			case zmqInfo.endTxnPuller:
				{
					req := &kpb.EndRequest{}
					data, _ := zmqInfo.endTxnPuller.RecvBytes(zmq.DONTWAIT)
					proto.Unmarshal(data, req)
					go endTxnHandler(keyNode, req)
				}
			}
		}
	}
}

func readHandler(keyNode *KeyNode, req *kpb.KeyNodeRequest) {
	start := time.Now()
	keyVersion, val, coWrites, err := keyNode.readKey(req.Tid, req.Key,
		req.ReadSet, req.BeginTs, req.LowerBound)
	end := time.Now()

	go keyNode.Monitor.TrackStat(req.GetTid(), "Read Key Time", end.Sub(start))

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
			Value:        val,
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

	go keyNode.Monitor.TrackStat(req.Tid, "Read response pusher time", end.Sub(start))
}

func validateHandler(keyNode *KeyNode, req *kpb.ValidateRequest) {
	start := time.Now()
	action := keyNode.validate(req.Tid, req.BeginTS, req.CommitTS, req.Keys)
	end := time.Now()

	go keyNode.Monitor.TrackStat(req.Tid, "Validation Time", end.Sub(start))

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

	go keyNode.Monitor.TrackStat(req.Tid, "Validation response pusher time", end.Sub(start))
}

func endTxnHandler(keyNode *KeyNode, req *kpb.EndRequest) {
	log.Debugf("Received end txn for %s", req.Tid)

	start := time.Now()
	err := keyNode.endTransaction(req.Tid, req.Action, req.WriteSet)
	end := time.Now()

	log.Debugf("Finished ending txn %s", req.Tid)

	go keyNode.Monitor.TrackStat(req.GetTid(), "End Transaction Time", end.Sub(start))

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
	addr := fmt.Sprintf(cmn.PushTemplate, req.IpAddress, cmn.TxnEndTxnPullPort)

	start = time.Now()
	keyNode.PusherCache.Lock(keyNode.ZmqInfo.context, addr)
	pusher := keyNode.PusherCache.GetSocket(addr)
	pusher.SendBytes(data, zmq.DONTWAIT)
	keyNode.PusherCache.Unlock(addr)
	end = time.Now()

	log.Debugf("Sent ACK to %s", addr)

	go keyNode.Monitor.TrackStat(req.Tid, "End txn response pusher time", end.Sub(start))
}
