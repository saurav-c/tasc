package main

import (
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"github.com/saurav-c/tasc/lib/routing"
	annapb "github.com/saurav-c/tasc/proto/anna"
	kpb "github.com/saurav-c/tasc/proto/keynode"
	tpb "github.com/saurav-c/tasc/proto/tasc"
	log "github.com/sirupsen/logrus"
	"time"
)

func (t *TxnManager) listener() {
	poller := zmq.NewPoller()
	info := t.ZmqInfo
	poller.Add(info.readPuller, zmq.POLLIN)
	poller.Add(info.valPuller, zmq.POLLIN)
	poller.Add(info.endTxnPuller, zmq.POLLIN)
	poller.Add(info.rtrPuller, zmq.POLLIN)

	for true {
		sockets, _ := poller.Poll(10 * time.Millisecond)

		for _, socket := range sockets {
			switch s := socket.Socket; s {
			case info.readPuller:
				{
					data, _ := info.readPuller.RecvBytes(zmq.DONTWAIT)
					go t.readHandler(data)
				}
			case info.valPuller:
				{
					data, _ := info.valPuller.RecvBytes(zmq.DONTWAIT)
					go t.validateHandler(data)
				}
			case info.endTxnPuller:
				{
					data, _ := info.endTxnPuller.RecvBytes(zmq.DONTWAIT)
					go t.endTxnHandler(data)
				}
			case info.rtrPuller:
				{
					data, _ := info.rtrPuller.RecvBytes(zmq.DONTWAIT)
					go t.routingHandler(data)
				}
			}
		}
	}
}

// Key Node Response Handler
func (t *TxnManager) readHandler(data []byte) {
	resp := &kpb.KeyNodeResponse{}
	err := proto.Unmarshal(data, resp)
	if err != nil {
		log.Error("Unable to parse KeyNode read response")
		return
	}
	tid := resp.GetTid()
	t.TransactionTable.mutex.RLock()
	channel := t.TransactionTable.table[tid].readChan
	t.TransactionTable.mutex.RUnlock()
	channel <- resp
}

func (t *TxnManager) validateHandler(data []byte) {
	resp := &kpb.ValidateResponse{}
	err := proto.Unmarshal(data, resp)
	if err != nil {
		log.Error("Unable to parse KeyNode validate response")
		return
	}
	tid := resp.GetTid()
	t.TransactionTable.mutex.RLock()
	channel := t.TransactionTable.table[tid].valChan
	t.TransactionTable.mutex.RUnlock()
	channel <- resp
}

func (t *TxnManager) endTxnHandler(data []byte) {
	resp := &tpb.TransactionTag{}
	err := proto.Unmarshal(data, resp)
	if err != nil {
		log.Error("Unable to parse worker ACK")
		return
	}
	tid := resp.GetTid()
	t.TransactionTable.mutex.RLock()
	channel := t.TransactionTable.table[tid].endTxnChan
	t.TransactionTable.mutex.RUnlock()
	channel <- resp
}

func (t *TxnManager) routingHandler(data [] byte) {
	resp := &annapb.KeyAddressResponse{}
	err := proto.Unmarshal(data, resp)
	if err != nil {
		log.Error("Unable to parse Router response")
		return
	}

	addrMap := make(map[string][]string)
	for _, r := range resp.Addresses {
		addrMap[r.Key] = r.Ips
	}
	rtrResp := &routing.RoutingResponse{Addresses: addrMap}

	tid := resp.GetResponseId()
	t.TransactionTable.mutex.RLock()
	channel := t.TransactionTable.table[tid].rtrChan
	t.TransactionTable.mutex.RUnlock()

	channel <- rtrResp
}