package main

import (
	"fmt"
	"github.com/pkg/errors"
	pb "github.com/saurav-c/aftsi/proto/aftsi/api"
	"hash"
	"os"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
	storage "github.com/saurav-c/aftsi/lib/storage"
	"github.com/golang/protobuf/proto"
	rpb "github.com/saurav-c/aftsi/proto/aftsi/replica"
	rtr "github.com/saurav-c/aftsi/proto/routing"
)

const (
	// Transaction State Status Codes
	TxnInProgress = 0
	TxnInValidation = 1
	TxnCommitted = 2
	TxnAborted = 3

	ReadCacheLimit int = 1000

	PullTemplate = "tcp://*:%d"
	PushTemplate = "tcp://%s:%d"

	// Txn Manager port for creating new entries
	createTxnPortReq = 5001
	createTxnPortResp = 5002

	// Router port for mapping TID to Txn Manager
	TxnRouterPullPort = 7000
	TxnRouterPushPort = 8000

	// Router port for mapping Key to Key Node
	KeyRouterPullPort = 7001
	KeyRouterPushPort = 8001

	// Key:Version Encoding
	keyVersionDelim = ':'
)

type TransactionEntry struct {
	beginTS          string
	endTS            string
	readSet          map[string]string
	coWrittenSet    map[string]string
	status           uint8
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
	WriteBuffer          map[string]map[string][]byte
	WriteBufferLock      map[string]*sync.RWMutex
	ReadCache            map[string][]byte
	ReadCacheLock        *sync.RWMutex
	zmqInfo              ZMQInfo
}

type ZMQInfo struct {
	context         *zmq.Context
	createTxnPuller *zmq.Socket
	txnRouterPuller *zmq.Socket
	txnRouterPusher *zmq.Socket
	keyRouterPuller *zmq.Socket
	keyRouterPusher *zmq.Socket
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
	req := &rtr.TxnRouterReq{
		Tid: tid,
	}
	data, _ := proto.Marshal(req)
	zmqInfo.txnRouterPusher.SendBytes(data, zmq.DONTWAIT)
	data, _ = zmqInfo.txnRouterPuller.RecvBytes(0)
	resp := &rtr.RouterResponse{}
	err := proto.Unmarshal(data, resp)
	if err != nil {
		return "", err
	}
	if resp.GetError() == rtr.RouterError_FAILURE {
		return resp.GetIp(), errors.New("Transaction router error")
	}
	return resp.GetIp(), nil
}

func pingKeyRouter(info *ZMQInfo, key string) (string, error) {
	req := &rtr.KeyRouterReq{
		Key: key,
	}
	data, _ := proto.Marshal(req)
	info.keyRouterPusher.SendBytes(data, zmq.DONTWAIT)
	data, _ = info.keyRouterPuller.RecvBytes(0)
	resp := &rtr.RouterResponse{}
	err := proto.Unmarshal(data, resp)
	if err != nil {
		return "", err
	}
	if resp.GetError() == rtr.RouterError_FAILURE {
		return resp.GetIp(), errors.New("Key router error")
	}
	return resp.GetIp(), nil
}

// Listens for incoming requests via ZMQ
func txnManagerListen(server *AftSIServer) {
	// Create a new poller to wait for new updates
	poller := zmq.NewPoller()

	poller.Add(server.zmqInfo.createTxnPuller, zmq.POLLIN)

	for true {
		sockets, _ := poller.Poll(10 * time.Millisecond)

		for _, socket := range sockets {
			switch s := socket.Socket; s {
			case server.zmqInfo.createTxnPuller:
				{
					req := &pb.CreateTxnEntry{}
					data, _ := server.zmqInfo.createTxnPuller.RecvBytes(zmq.DONTWAIT)
					proto.Unmarshal(data, req)
					go server.CreateTransactionEntry(req.GetTid(), req.GetTxnManagerIP())
				}
			}
		}
	}
}

func NewAftSIServer(txnRouterIP string, keyRouterIP string) (*AftSIServer, int, error) {
	zctx, err := zmq.NewContext()
	if err != nil {
		return nil, 0, err
	}

	// Setup Txn Manager ZMQ sockets
	createTxnPuller := createSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, createTxnPortReq), true)

	// Setup routing ZMQ sockets
	txnRouterPuller := createSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, TxnRouterPullPort), true)
	txnRouterPusher := createSocket(zmq.PUSH, zctx, fmt.Sprintf(PushTemplate, txnRouterIP, TxnRouterPushPort), false)

	keyRouterPuller := createSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, KeyRouterPullPort), true)
	keyRouterPusher := createSocket(zmq.PUSH, zctx, fmt.Sprintf(PushTemplate, txnRouterIP, KeyRouterPushPort), false)

	zmqInfo := ZMQInfo{
		context:         zctx,
		createTxnPuller: createTxnPuller,
		txnRouterPuller: txnRouterPuller,
		txnRouterPusher: txnRouterPusher,
		keyRouterPuller: keyRouterPuller,
		keyRouterPusher: keyRouterPusher,
	}

	return &AftSIServer{
		counter:              0,
		counterMutex:         &sync.Mutex{},
		IPAddress:            "",
		serverID:             "",
		StorageManager:       nil,
		TransactionTable:     make(map[string]*TransactionEntry),
		TransactionTableLock: make(map[string]*sync.RWMutex),
		WriteBuffer:          make(map[string]map[string][]byte),
		WriteBufferLock:      make(map[string]*sync.RWMutex),
		ReadCache:            make(map[string][]byte),
		ReadCacheLock:        &sync.RWMutex{},
		zmqInfo:              zmqInfo,
	}, 0, nil
}
