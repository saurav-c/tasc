package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	pb "github.com/saurav-c/aftsi/proto/keynode/api"
	"github.com/golang/protobuf/proto"
	"log"
	"os"
	"sync"
	storage "github.com/saurav-c/aftsi/lib/storage"
	"time"
)

const (
	ReadCacheLimit = 1000

	// Key Node puller ports
	readPullPort = 6000
	validatePullPort = 6001
	endTxnPort = 6002

	// Txn Manager pusher ports
	readRespPort = 9000
	valRespPort = 9001
	endTxnRespPort = 9002

	PullTemplate = "tcp://*:%d"
	PushTemplate = "tcp://%s:%d"
)

func findIndex (a []string, e string) (int) {
	for index, elem := range a {
		if elem == e {
			return index
		}
	}
	return -1
}

func intersection (a []string, b []string) (bool) {
	var hash map[string]bool
	for _, elem := range a {
		hash[elem] = true
	}
	for _, elem := range b {
		if hash[elem] {
			return true
		}
	}
	return false
}

func InsertParticularIndex(list []*keyVersion, kv *keyVersion) []*keyVersion {
	if len(list) == 0 {
		return []*keyVersion{kv}
	}
	index := FindIndex(list, kv)
	return append(append(list[:index], kv), list[index:]...)
}

func FindIndex(list []*keyVersion, kv *keyVersion) int {
	startList := 0
	endList := len(list) - 1
	midPoint := 0
	for true {
		midPoint = (startList + endList) / 2
		midElement := list[midPoint]
		if midElement.CommitTS == kv.CommitTS {
			return midPoint
		} else if kv.CommitTS < list[startList].CommitTS {
			return startList
		} else if kv.CommitTS > list[endList].CommitTS {
			return endList
		} else if midElement.CommitTS > kv.CommitTS {
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

type keyVersion struct {
	tid      string
	CommitTS string
}

type pendingTxn struct {
	keys []string
	commitTS string
}

type KeyNode struct {
	StorageManager              storage.StorageManager
	keyVersionIndex             map[string][]*keyVersion
	keyVersionIndexLock         map[string]*sync.RWMutex
	pendingKeyVersionIndex      map[string][]*keyVersion
	pendingKeyVersionIndexLock  map[string]*sync.RWMutex
	pendingTxnCache             map[string]*pendingTxn
	committedTxnCache           map[string][]string
	readCache                   map[string][]byte
	readCacheLock               *sync.RWMutex
	zmqInfo						ZMQInfo
}

type ZMQInfo struct {
	context         *zmq.Context
	readPuller *zmq.Socket
	validatePuller *zmq.Socket
	endTxnPuller *zmq.Socket
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
	tid, keyVersion, val, coWrites, err := keyNode.readKey(req.GetTid(),
		req.GetKey(), req.GetReadSet(), req.GetBeginTS(), req.GetLowerBound())

	var resp *pb.KeyResponse

	// Create ZMQ Socket to send response
	pusher := createSocket(zmq.PUSH, keyNode.zmqInfo.context, fmt.Sprintf(PushTemplate, req.GetTxnMngrIP(), readRespPort), false)
	defer pusher.Close()

	if err != nil {
		resp = &pb.KeyResponse{
			Error: pb.KeyError_FAILURE,
		}
	} else {
		resp = &pb.KeyResponse{
			Tid:                  tid,
			KeyVersion:           keyVersion,
			Value:                val,
			CoWrittenSet:         coWrites,
			ChannelID: 			  req.GetChannelID(),
			Error:                pb.KeyError_SUCCESS,
		}
	}

	data, _ := proto.Marshal(resp)
	pusher.SendBytes(data, zmq.DONTWAIT)
}

func validateHandler(keyNode *KeyNode, req *pb.ValidateRequest) {
	tid, action := keyNode.validate(req.GetTid(), req.GetBeginTS(), req.GetCommitTS(), req.GetKeys())

	var resp *pb.ValidateResponse

	// Create ZMQ Socket to send response
	pusher := createSocket(zmq.PUSH, keyNode.zmqInfo.context, fmt.Sprintf(PushTemplate, req.GetTxnMngrIP(), valRespPort), false)
	defer pusher.Close()

	var ok bool
	if action == TRANSACTION_SUCCESS {
		ok = true
	} else {
		ok = false
	}

	resp = &pb.ValidateResponse{
		Tid:       tid,
		Ok:        ok,
		ChannelID: req.GetChannelID(),
		Error:     pb.KeyError_SUCCESS,
	}

	data, _ := proto.Marshal(resp)
	pusher.SendBytes(data, zmq.DONTWAIT)
}

func endTxnHandler(keyNode *KeyNode, req *pb.FinishRequest) {
	var action int
	if req.GetS() == pb.TransactionAction_COMMIT {
		action = 0
	} else {
		action = 1
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
		ChannelID: req.GetChannelID(),
		Error:     e,
	}

	pusher := createSocket(zmq.PUSH, keyNode.zmqInfo.context, fmt.Sprintf(PushTemplate, req.GetTxnMngrIP(), endTxnRespPort), false)
	defer pusher.Close()

	data, _ := proto.Marshal(resp)
	pusher.SendBytes(data, zmq.DONTWAIT)
}

func NewKeyNode(KeyNodeIP string, storageInstance string) (*KeyNode, error) {
	// TODO: Integrate this into config manager
	// Need to change parameters to fit around needs better
	var storageManager storage.StorageManager
	switch storageInstance {
	case "dynamo":
		storageManager = storage.NewDynamoStorageManager("AftSiData", "AftSiData")
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

	return &KeyNode{
		StorageManager:             storageManager,
		keyVersionIndex:            make(map[string][]*keyVersion),
		keyVersionIndexLock:        make(map[string]*sync.RWMutex),
		pendingKeyVersionIndex:     make(map[string][]*keyVersion),
		pendingKeyVersionIndexLock: make(map[string]*sync.RWMutex),
		committedTxnCache:          make(map[string][]string),
		readCache:                  make(map[string][]byte),
		readCacheLock:              &sync.RWMutex{},
		zmqInfo:					zmqInfo,
	}, nil
}
