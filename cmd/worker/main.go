package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	cmn "github.com/saurav-c/tasc/lib/common"
	"github.com/saurav-c/tasc/lib/routing"
	annapb "github.com/saurav-c/tasc/proto/anna"
	kpb "github.com/saurav-c/tasc/proto/keynode"
	tpb "github.com/saurav-c/tasc/proto/tasc"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
	"time"
)

func (w *TxnWorker) listen() {
	poller := zmq.NewPoller()
	info := w.ZMQInfo
	poller.Add(info.txnPuller, zmq.POLLIN)
	poller.Add(info.rtrPuller, zmq.POLLIN)

	for true {
		sockets, _ := poller.Poll(10 * time.Millisecond)

		for _, socket := range sockets {
			switch s := socket.Socket; s {
			case info.txnPuller:
				{
					data, _ := info.txnPuller.RecvBytes(zmq.DONTWAIT)
					go w.handler(data)
				}
			case info.rtrPuller:
				{
					data, _ := info.rtrPuller.RecvBytes(zmq.DONTWAIT)
					go w.rtrHandler(data)
				}
			}
		}
	}
}

func (w *TxnWorker) handler(data []byte) {
	start := time.Now()

	txn := &tpb.TransactionResult{}
	err := proto.Unmarshal(data, txn)
	if err != nil {
		log.Println("Unable to parse transaction result")
		return
	}
	tid := txn.Tag.Tid

	// Send ACK to Txn Manager
	txnIP := txn.Tag.TxnManagerIP
	txnAddr := fmt.Sprintf(cmn.PushTemplate, txnIP, cmn.TxnAckPullPort)

	log.WithFields(log.Fields{
		"TID": tid,
		"Txn Manager": txnIP,
		"MSG": "Received End Txn Request",
	}).Debug()

	writeStart := time.Now()
	// Mimic storage write
	w.StorageManager.Put(tid + "active-worker", []byte("ACK"))
	writeEnd := time.Now()
	go w.Monitor.TrackStat(tid, "[END] Worker Storage ACK", writeEnd.Sub(writeStart))

	data, _ = proto.Marshal(txn.Tag)
	w.PusherCache.Lock(w.ZMQInfo.context, txnAddr)
	pusher := w.PusherCache.GetSocket(txnAddr)
	pusher.SendBytes(data, zmq.DONTWAIT)
	w.PusherCache.Unlock(txnAddr)

	end := time.Now()
	go w.Monitor.TrackStat(tid, "[END] Observed Worker", end.Sub(start))

	log.WithFields(log.Fields{
		"TID": tid,
		"Txn Manager": txnIP,
		"MSG": "Responded with ACK",
	}).Debug()

	// Lookup relevant Key Nodes
	c := make(chan *routing.RoutingResponse)
	w.RtrChanMutex.Lock()
	w.RtrChanMap[txn.Tag.Tid] = c
	w.RtrChanMutex.Unlock()

	// Convert keyversions to keys
	var keys []string
	for _, kv := range txn.Writeset.Keys {
		split := strings.Split(kv, cmn.KeyDelimeter)
		keys = append(keys, split[0])
	}

	w.RouterManager.Lookup(tid, keys)

	resp := <-c
	keyToNode := resp.Addresses
	nodeToKey := make(map[string]string)
	for _, addrs := range keyToNode {
		ip := addrs[0]
		ip = ip[:len(ip)-1]
		if _, ok := nodeToKey[ip]; !ok {
			nodeToKey[ip] = ""
		}
	}

	var action kpb.TransactionAction
	if txn.Tag.Status == tpb.TascTransactionStatus_COMMITTED {
		action = kpb.TransactionAction_COMMIT
	} else {
		action = kpb.TransactionAction_ABORT
	}

	// Send end transaction messages
	for keyNode, _ := range nodeToKey {
		log.Debugf("Sending end txn status to keynode %s", keyNode)
		go w.endTransaction(tid, keyNode, txn.Writeset.Keys, action)
	}

	// Resend if ACK not received
}

func (w *TxnWorker) endTransaction(tid string, nodeAddr string, keys []string, action kpb.TransactionAction) {
	addr := fmt.Sprintf("%s:%d", nodeAddr, cmn.KeyEndTxnPullPort)
	endReq := &kpb.EndRequest{
		Tid:       tid,
		Action:    action,
		WriteSet:  keys,
		IpAddress: w.IpAddress,
	}
	data, _ := proto.Marshal(endReq)
	w.PusherCache.Lock(w.ZMQInfo.context, addr)
	endPusher := w.PusherCache.GetSocket(addr)
	endPusher.SendBytes(data, zmq.DONTWAIT)
	w.PusherCache.Unlock(addr)

	log.Debugf("Sent status to %s", addr)
}

func (w *TxnWorker) rtrHandler(data []byte) {
	resp := &annapb.KeyAddressResponse{}
	err := proto.Unmarshal(data, resp)
	if err != nil {
		log.Println("Unable to parse Router response")
		return
	}

	addrMap := make(map[string][]string)
	for _, r := range resp.Addresses {
		addrMap[r.Key] = r.Ips
	}
	rtrResp := &routing.RoutingResponse{Addresses: addrMap}

	tid := resp.ResponseId
	w.RtrChanMutex.RLock()
	c := w.RtrChanMap[tid]
	w.RtrChanMutex.RUnlock()

	c <- rtrResp
}

func main() {
	worker, err := NewTransactionWorker()
	if err != nil {
		log.Fatal("Unable to create transaction worker: " + err.Error())
		os.Exit(1)
	}

	log.Infof("Started worker at " + worker.IpAddress)

	go worker.Monitor.SendStats(5 * time.Second)
	// Start listening for transaction updates
	worker.listen()
}