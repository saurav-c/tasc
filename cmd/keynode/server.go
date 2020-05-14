package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"github.com/saurav-c/aftsi/lib/storage"
	pb "github.com/saurav-c/aftsi/proto/keynode/api"
	"log"
	"os"
	"sync"
	"time"
)

const (
	ReadCacheLimit = 1000

	// Key Node puller ports
	readPullPort     = 6000
	validatePullPort = 6001
	endTxnPort       = 6002

	// Txn Manager pusher ports
	readRespPort   = 9000
	valRespPort    = 9001
	endTxnRespPort = 9002

	PullTemplate = "tcp://*:%d"
	PushTemplate = "tcp://%s:%d"

	// In Seconds
	FlushFrequency = 1000
)

type pendingTxn struct {
	keys       []string
	keyVersion string
}

type keysList struct {
	keys []string
}

type KeyNode struct {
	StorageManager             storage.StorageManager
	keyVersionIndex            map[string]*keysList
	keyVersionIndexLock        *sync.RWMutex
	committedKeysLock          map[string]*sync.RWMutex
	committedLock              *sync.RWMutex
	pendingKeyVersionIndex     map[string]*keysList
	pendingKeyVersionIndexLock *sync.RWMutex
	pendingKeysLock            map[string]*sync.RWMutex
	pendingLock                *sync.RWMutex
	pendingTxnCache            map[string]*pendingTxn
	pendingTxnCacheLock        *sync.RWMutex
	committedTxnCache          map[string][]string
	committedTxnCacheLock      *sync.RWMutex
	readCache                  map[string][]byte
	readCacheLock              *sync.RWMutex
	commitBuffer               map[string][]byte
	commitLock                 *sync.RWMutex
	zmqInfo                    ZMQInfo
	pusherCache                *SocketCache
	batchMode                  bool
}

type ZMQInfo struct {
	context        *zmq.Context
	readPuller     *zmq.Socket
	validatePuller *zmq.Socket
	endTxnPuller   *zmq.Socket
}

type SocketCache struct {
	locks       map[string]*sync.Mutex
	sockets     map[string]*zmq.Socket
	lockMutex   *sync.RWMutex
	socketMutex *sync.RWMutex
}

func (cache *SocketCache) lock(ctx *zmq.Context, address string) {
	cache.lockMutex.Lock()
	_, ok := cache.locks[address]
	if !ok {
		cache.socketMutex.Lock()
		cache.locks[address] = &sync.Mutex{}
		cache.sockets[address] = createSocket(zmq.PUSH, ctx, address, false)
		cache.socketMutex.Unlock()
	}
	addrLock := cache.locks[address]
	cache.lockMutex.Unlock()
	addrLock.Lock()
}


func (cache *SocketCache) unlock(address string) {
	cache.lockMutex.RLock()
	cache.locks[address].Unlock()
	cache.lockMutex.RUnlock()
}

// Lock and Unlock need to be called on the Cache for this address
func (cache *SocketCache) getSocket(address string) *zmq.Socket {
	cache.socketMutex.RLock()
	socket := cache.sockets[address]
	cache.socketMutex.RUnlock()
	return socket
}

func InsertParticularIndex(list []string, kv string) []string {
	if len(list) == 0 {
		return []string{kv}
	}
	index := FindIndex(list, kv)
	return append(append(list[:index], kv), list[index:]...)
}

func FindIndex(list []string, kv string) int {
	startList := 0
	endList := len(list) - 1
	midPoint := 0
	for true {
		midPoint = (startList + endList) / 2
		midElement := list[midPoint]
		if midElement == kv {
			return midPoint
		} else if kv < list[startList] {
			return startList
		} else if kv > list[endList] {
			return endList
		} else if midElement > kv {
			startList = midPoint
		} else {
			endList = midPoint
		}
	}
	return -1
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

func flusher(k *KeyNode) {
	for {
		if len(k.commitBuffer) > 0 {
			e := k._flushBuffer()
			if e != nil {
				fmt.Println(e.Error())
			}
		}
		time.Sleep(FlushFrequency * time.Millisecond)
	}
}

func startKeyNode(keyNode *KeyNode) {
	poller := zmq.NewPoller()
	zmqInfo := keyNode.zmqInfo

	poller.Add(zmqInfo.readPuller, zmq.POLLIN)
	poller.Add(zmqInfo.validatePuller, zmq.POLLIN)
	poller.Add(zmqInfo.endTxnPuller, zmq.POLLIN)

	for true {
		sockets, _ := poller.Poll(10 * time.Millisecond)

		for _, socket := range sockets {
			switch s := socket.Socket; s {
			case zmqInfo.readPuller:
				{
					req := &pb.KeyRequest{}
					data, _ := zmqInfo.readPuller.RecvBytes(zmq.DONTWAIT)
					proto.Unmarshal(data, req)
					go readHandler(keyNode, req)
				}
			case zmqInfo.validatePuller:
				{
					req := &pb.ValidateRequest{}
					data, _ := zmqInfo.validatePuller.RecvBytes(zmq.DONTWAIT)
					proto.Unmarshal(data, req)
					go validateHandler(keyNode, req)
				}
			case zmqInfo.endTxnPuller:
				{
					req := &pb.FinishRequest{}
					data, _ := zmqInfo.endTxnPuller.RecvBytes(zmq.DONTWAIT)
					proto.Unmarshal(data, req)
					go endTxnHandler(keyNode, req)
				}
			}
		}
	}
}

func readHandler(keyNode *KeyNode, req *pb.KeyRequest) {
	keyVersion, val, coWrites, err := keyNode.readKey(req.GetTid(),
		req.GetKey(), req.GetReadSet(), req.GetBeginTS(), req.GetLowerBound())

	var resp *pb.KeyResponse

	if err != nil {
		resp = &pb.KeyResponse{
			Error:     pb.KeyError_FAILURE,
			ChannelID: req.GetChannelID(),
		}
	} else {
		resp = &pb.KeyResponse{
			Tid:          req.GetTid(),
			KeyVersion:   keyVersion,
			Value:        val,
			CoWrittenSet: coWrites,
			Error:        pb.KeyError_SUCCESS,
			ChannelID:    req.GetChannelID(),
		}
	}

	data, _ := proto.Marshal(resp)
	addr := fmt.Sprintf(PushTemplate, req.GetTxnMngrIP(), readRespPort)
	keyNode.pusherCache.lock(keyNode.zmqInfo.context, addr)
	pusher := keyNode.pusherCache.getSocket(addr)
	pusher.SendBytes(data, zmq.DONTWAIT)
	keyNode.pusherCache.unlock(addr)
}

func validateHandler(keyNode *KeyNode, req *pb.ValidateRequest) {
	action := keyNode.validate(req.GetTid(), req.GetBeginTS(), req.GetCommitTS(), req.GetKeys())

	var resp *pb.ValidateResponse

	var ok bool
	if action == TRANSACTION_SUCCESS {
		ok = true
	} else {
		ok = false
	}

	resp = &pb.ValidateResponse{
		Tid:       req.GetTid(),
		Ok:        ok,
		Error:     pb.KeyError_SUCCESS,
		ChannelID: req.GetChannelID(),
	}
	data, _ := proto.Marshal(resp)

	addr := fmt.Sprintf(PushTemplate, req.GetTxnMngrIP(), valRespPort)
	keyNode.pusherCache.lock(keyNode.zmqInfo.context, addr)
	pusher := keyNode.pusherCache.getSocket(addr)
	pusher.SendBytes(data, zmq.DONTWAIT)
	keyNode.pusherCache.unlock(addr)
}

func endTxnHandler(keyNode *KeyNode, req *pb.FinishRequest) {
	var action int8
	if req.GetS() == pb.TransactionAction_COMMIT {
		action = TRANSACTION_SUCCESS
	} else {
		action = TRANSACTION_FAILURE
	}

	writeMap := make(map[string][]byte)
	set := req.GetWriteSet()
	buffer := req.GetWriteBuffer()

	for i, key := range set {
		writeMap[key] = buffer[i]
	}

	err := keyNode.endTransaction(req.GetTid(), action, writeMap)

	var e pb.KeyError
	if err != nil {
		e = pb.KeyError_FAILURE
	} else {
		e = pb.KeyError_SUCCESS
	}

	resp := &pb.FinishResponse{
		Tid:       req.GetTid(),
		Error:     e,
		ChannelID: req.GetChannelID(),
	}

	data, _ := proto.Marshal(resp)

	addr := fmt.Sprintf(PushTemplate, req.GetTxnMngrIP(), endTxnRespPort)
	keyNode.pusherCache.lock(keyNode.zmqInfo.context, addr)
	pusher := keyNode.pusherCache.getSocket(addr)
	pusher.SendBytes(data, zmq.DONTWAIT)
	keyNode.pusherCache.unlock(addr)
}

func NewKeyNode(storageInstance string, batchMode bool) (*KeyNode, error) {
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

	zctx, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}

	readPuller := createSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, readPullPort), true)
	validatePuller := createSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, validatePullPort), true)
	endTxnPuller := createSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, endTxnPort), true)

	zmqInfo := ZMQInfo{
		context:        zctx,
		readPuller:     readPuller,
		validatePuller: validatePuller,
		endTxnPuller:   endTxnPuller,
	}

	pusherCache := SocketCache{
		locks:       make(map[string]*sync.Mutex),
		sockets:     make(map[string]*zmq.Socket),
		lockMutex:   &sync.RWMutex{},
		socketMutex: &sync.RWMutex{},
	}

	return &KeyNode{
		StorageManager:             storageManager,
		keyVersionIndex:            make(map[string]*keysList),
		keyVersionIndexLock:        &sync.RWMutex{},
		committedKeysLock:          make(map[string]*sync.RWMutex),
		committedLock:              &sync.RWMutex{},
		pendingKeyVersionIndex:     make(map[string]*keysList),
		pendingKeyVersionIndexLock: &sync.RWMutex{},
		pendingKeysLock:            make(map[string]*sync.RWMutex),
		pendingLock:                &sync.RWMutex{},
		committedTxnCache:          make(map[string][]string),
		committedTxnCacheLock:      &sync.RWMutex{},
		pendingTxnCache:            make(map[string]*pendingTxn),
		pendingTxnCacheLock:        &sync.RWMutex{},
		readCache:                  make(map[string][]byte),
		readCacheLock:              &sync.RWMutex{},
		commitBuffer:               make(map[string][]byte),
		commitLock:                 &sync.RWMutex{},
		zmqInfo:                    zmqInfo,
		pusherCache:                &pusherCache,
		batchMode:                  batchMode,
	}, nil
}
