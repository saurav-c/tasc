package main

import (
	"fmt"
	"hash"
	"os"
	"sync"

	zmq "github.com/pebbe/zmq4"
	storage "github.com/saurav-c/aftsi/lib/storage"
	"github.com/golang/protobuf/proto"
	rpb "github.com/saurav-c/aftsi/proto/aftsi/replica"
	rtr "github.com/saurav-c/aftsi/proto/routing"
)

const (
	ReadCacheLimit int = 1000

	PullTemplate = "tcp://*:%d"
	PushTemplate = "tcp://%s:%d"

	// Router port for mapping TID tp Txn Manager
	TxnRouterPullPort = 7000
	TxnRouterPushPort = 8000
)

type TransactionEntry struct {
	beginTS          string
	endTS            string
	readSet          map[string]string
	coWrittenSets    map[string][]string
	status           uint32
	unverifiedProtos map[hash.Hash]*rpb.TransactionUpdate
}

type AftSIServer struct {
	counter              uint64
	counterMutex         *sync.Mutex
	IPAddress            string
	serverID             string
	StorageManager       storage.StorageManager
	TransactionTable     map[string]*TransactionEntry
	TransactionTableLock map[string]*sync.RWMutex
	WriteBuffer          map[string]map[string]byte
	WriteBufferLock      map[string]*sync.RWMutex
	ReadCache            map[string]byte
	ReadCacheLock        *sync.Mutex
	zmqInfo              ZMQInfo
}

type ZMQInfo struct {
	context         *zmq.Context
	txnRouterPuller *zmq.Socket
	txnRouterPusher *zmq.Socket
}

func createSocket(tp zmq.Type, context *zmq.Context, address string, bind bool) *zmq.Socket {
	sckt, err := context.NewSocket(tp)
	if err != nil {
		fmt.Println("Unexpected error while creating new socket:\n", err)
		os.Exit(1)
	}

	if bind {
		err = sckt.Bind(address)
	} else {
		err = sckt.Connect(address)
	}

	if err != nil {
		fmt.Println("Unexpected error while binding/connecting socket:\n", err)
		os.Exit(1)
	}

	return sckt
}

func pingTxnRouter(zmqInfo *ZMQInfo, tid string) (string, error) {
	req := &rtr.TxnMngrRequest{tid: tid}
	data, _ := proto.Marshal(req)
	zmqInfo.txnRouterPusher.SendBytes(data)
	data, _ = zmqInfo.txnRouterPuller.RecvBytes()
	resp := &rtr.TxnMngrResponse{}
	err := proto.Unmarshal(data, resp)
	if err != nil {
		return nil, err
	}
	return resp.Ip, nil
}

func NewAftSIServer(routerIP string) (*AftSIServer, int, error) {
	zctx, err := zmq.NewContext()
	if err != nil {
		return nil, nil, err
	}

	txnRouterPuller := createSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, TxnRouterPullPort), true)
	txnRouterPusher := createSocket(zmq.PUSH, zctx, fmt.Sprintf(PushTemplate, routerIP, TxnRouterPushPort), false)

	zmqInfo = ZMQInfo{context: zctx, txnRouterPuller: txnRouterPuller, txnRouterPuller: txnRouterPusher}
	
	return AftSIServer {
		counter: 0, 
		IPAddress: "", 
		serverID: "", 
		StorageManager: nil, 
		TransactionTable: make(map[string]*TransactionEntry),
		TransactionTableLock: make(map[string]*sync.RWMutex),
		WriteBuffer: make(map[string]map[string]byte),
		WriteBufferLock: make(map[string]*sync.RWMutex),
		ReadCache: make(map[string]byte),
		zmqInfo: zmqInfo
	}
}































