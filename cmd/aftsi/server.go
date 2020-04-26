package main

import (
	"fmt"
	"github.com/pkg/errors"
	pb "github.com/saurav-c/aftsi/proto/aftsi/api"
	"log"
	"os"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	storage "github.com/saurav-c/aftsi/lib/storage"
	key "github.com/saurav-c/aftsi/proto/keynode/api"
	rtr "github.com/saurav-c/aftsi/proto/routing"
)

const (
	// Transaction State Status Codes
	TxnInProgress   = 0
	TxnInValidation = 1
	TxnCommitted    = 2
	TxnAborted      = 3

	ReadCacheLimit int = 1000

	PullTemplate = "tcp://*:%d"
	PushTemplate = "tcp://%s:%d"

	// Txn Manager port for creating new entries
	createTxnPortReq  = 5001
	createTxnPortResp = 5002

	// Router port for mapping TID to Txn Manager
	TxnRouterPullPort = 7000
	TxnRouterPushPort = 8000

	// Router port for mapping Key to Key Node
	KeyRouterPullPort = 7001
	KeyRouterPushPort = 8001

	// Key Node response ports
	ReadRespPullPort = 9000
	ValRespPullPort  = 9001
	EndTxnRespPort   = 9002

	// Key Node puller ports
	readPullPort     = 6000
	validatePullPort = 6001
	endTxnPort       = 6002

	// Key:Version Encoding
	keyVersionDelim = ":"
)

type TransactionEntry struct {
	beginTS      string
	endTS        string
	readSet      map[string]string
	coWrittenSet map[string]string
	status       uint8
	// unverifiedProtos map[hash.Hash]*rpb.TransactionUpdate
}

type AftSIServer struct {
	counter              uint64
	counterMutex         *sync.Mutex
	IPAddress            string
	KeyNodeIP            string
	serverID             string
	StorageManager       storage.StorageManager
	TransactionTable     map[string]*TransactionEntry
	TransactionTableLock map[string]*sync.RWMutex
	WriteBuffer          map[string]map[string][]byte
	WriteBufferLock      map[string]*sync.RWMutex
	ReadCache            map[string][]byte
	ReadCacheLock        *sync.RWMutex
	zmqInfo              ZMQInfo
	KeyResponder         *KeyNodeResponse
	PusherCache          *SocketCache
}

type ZMQInfo struct {
	context         *zmq.Context
	createTxnPuller *zmq.Socket
	txnRouterPuller *zmq.Socket
	txnRouterPusher *zmq.Socket
	keyRouterPuller *zmq.Socket
	keyRouterPusher *zmq.Socket
	readPuller      *zmq.Socket
	valPuller       *zmq.Socket
	endTxnPuller    *zmq.Socket
}

type KeyNodeResponse struct {
	readChannels     map[uint32](chan *key.KeyResponse)
	validateChannels map[uint32](chan *key.ValidateResponse)
	endTxnChannels   map[uint32](chan *key.FinishResponse)
}

type SocketCache struct {
	locks        map[string]*sync.Mutex
	sockets      map[string]*zmq.Socket
	creatorMutex *sync.Mutex
}

func (cache *SocketCache) lock(ctx *zmq.Context, address string) {
	if _, ok := cache.locks[address]; ok {
		cache.locks[address].Lock()
		return
	}

	cache.creatorMutex.Lock()
	// Check again for race condition
	if _, ok := cache.locks[address]; !ok {
		cache.locks[address] = &sync.Mutex{}
		cache.sockets[address] = createSocket(zmq.PUSH, ctx, address, false)
	}
	cache.locks[address].Lock()
	cache.creatorMutex.Unlock()
}

func (cache *SocketCache) unlock(address string) {
	cache.locks[address].Unlock()
}

// Lock and Unlock need to be called on the Cache for this address
func (cache *SocketCache) getSocket(address string) *zmq.Socket {
	return cache.sockets[address]
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

	info := server.zmqInfo

	poller.Add(info.createTxnPuller, zmq.POLLIN)

	for true {
		sockets, _ := poller.Poll(10 * time.Millisecond)

		for _, socket := range sockets {
			switch s := socket.Socket; s {
			case info.createTxnPuller:
				{
					req := &pb.CreateTxnEntry{}
					data, _ := info.createTxnPuller.RecvBytes(zmq.DONTWAIT)
					proto.Unmarshal(data, req)
					go server.CreateTransactionEntry(req.GetTid(), req.GetTxnManagerIP())
				}
			case info.readPuller:
				{
					data, _ := info.readPuller.RecvBytes(zmq.DONTWAIT)
					go readHandler(data, server.KeyResponder)
				}
			case info.valPuller:
				{
					data, _ := info.valPuller.RecvBytes(zmq.DONTWAIT)
					go validateHandler(data, server.KeyResponder)
				}
			case info.endTxnPuller:
				{
					data, _ := info.endTxnPuller.RecvBytes(zmq.DONTWAIT)
					go endTxnHandler(data, server.KeyResponder)
				}
			}
		}
	}
}

// Key Node Response Handlers

func readHandler(data []byte, keyNodeResponder *KeyNodeResponse) {
	resp := &key.KeyResponse{}
	proto.Unmarshal(data, resp)
	channelID := resp.GetChannelID()
	keyNodeResponder.readChannels[channelID] <- resp
}

func validateHandler(data []byte, keyNodeResponder *KeyNodeResponse) {
	resp := &key.ValidateResponse{}
	proto.Unmarshal(data, resp)
	channelID := resp.GetChannelID()
	keyNodeResponder.validateChannels[channelID] <- resp
}

func endTxnHandler(data []byte, keyNodeResponder *KeyNodeResponse) {
	resp := &key.FinishResponse{}
	proto.Unmarshal(data, resp)
	channelID := resp.GetChannelID()
	keyNodeResponder.endTxnChannels[channelID] <- resp
}

func NewAftSIServer(personalIP string, txnRouterIP string, keyRouterIP string, keyNodeIP string, storageInstance string, testInstance bool) (*AftSIServer, int, error) {
	zctx, err := zmq.NewContext()
	if err != nil {
		return nil, 0, err
	}

	// TODO: Integrate this into config manager
	// Need to change parameters to fit around needs better
	var storageManager storage.StorageManager
	switch storageInstance {
	case "dynamo":
		storageManager = storage.NewDynamoStorageManager("Aftsi", "Aftsi")
	case "local":
		storageManager = storage.NewLocalStoreManager()
	default:
		log.Fatal(fmt.Sprintf("Unrecognized storageType %s. Valid types are: s3, dynamo, redis.", storageInstance))
		os.Exit(3)
	}

	// Setup Txn Manager ZMQ sockets
	createTxnPuller := createSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, createTxnPortReq), true)

	// Setup Key Node sockets
	readPuller := createSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, ReadRespPullPort), true)
	validatePuller := createSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, ValRespPullPort), true)
	endTxnPuller := createSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, EndTxnRespPort), true)

	zmqInfo := ZMQInfo{
		context:         zctx,
		createTxnPuller: createTxnPuller,
		readPuller:      readPuller,
		valPuller:       validatePuller,
		endTxnPuller:    endTxnPuller,
	}

	keyResponder := KeyNodeResponse{
		readChannels:     make(map[uint32](chan *key.KeyResponse)),
		validateChannels: make(map[uint32](chan *key.ValidateResponse)),
		endTxnChannels:   make(map[uint32](chan *key.FinishResponse)),
	}

	pusherCache := SocketCache{
		locks:        make(map[string]*sync.Mutex),
		sockets:      make(map[string]*zmq.Socket),
		creatorMutex: &sync.Mutex{},
	}

	return &AftSIServer{
		counter:              0,
		counterMutex:         &sync.Mutex{},
		IPAddress:            personalIP,
		KeyNodeIP:            keyNodeIP,
		serverID:             "",
		StorageManager:       storageManager,
		TransactionTable:     make(map[string]*TransactionEntry),
		TransactionTableLock: make(map[string]*sync.RWMutex),
		WriteBuffer:          make(map[string]map[string][]byte),
		WriteBufferLock:      make(map[string]*sync.RWMutex),
		ReadCache:            make(map[string][]byte),
		ReadCacheLock:        &sync.RWMutex{},
		zmqInfo:              zmqInfo,
		KeyResponder:         &keyResponder,
		PusherCache:          &pusherCache,
	}, 0, nil
}
