package main

import (
	"fmt"
	cmn "github.com/saurav-c/aftsi/lib/common"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"github.com/saurav-c/aftsi/config"
	"github.com/saurav-c/aftsi/lib/storage"
	pb "github.com/saurav-c/aftsi/proto/keynode"
	mpb "github.com/saurav-c/aftsi/proto/monitor"
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
	pusherCache                *cmn.SocketCache
	logFile                    *os.File
	monitor                    *cmn.StatsMonitor
}

type ZMQInfo struct {
	context        *zmq.Context
	readPuller     *zmq.Socket
	validatePuller *zmq.Socket
	endTxnPuller   *zmq.Socket
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

func startKeyNode(keyNode *KeyNode) {
	go keyNode.monitor.SendStats(1 * time.Second)

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
					req := &pb.KeyNodeRequest{}
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

func readHandler(keyNode *KeyNode, req *pb.KeyNodeRequest) {
	start := time.Now()
	keyVersion, val, coWrites, err := keyNode.readKey(req.GetTid(),
		req.GetKey(), req.GetReadSet(), req.GetBeginTS(), req.GetLowerBound())
	end := time.Now()

	go keyNode.monitor.TrackStat(req.GetTid(), "Read Key Time", end.Sub(start))

	var resp *pb.KeyNodeResponse

	if err != nil {
		resp = &pb.KeyNodeResponse{
			Error:     pb.KeyError_K_FAILURE,
			ChannelID: req.GetChannelID(),
		}
	} else {
		resp = &pb.KeyNodeResponse{
			Tid:          req.GetTid(),
			KeyVersion:   keyVersion,
			Value:        val,
			CoWrittenSet: coWrites,
			Error:        pb.KeyError_K_SUCCESS,
			ChannelID:    req.GetChannelID(),
		}
	}

	data, _ := proto.Marshal(resp)
	addr := fmt.Sprintf(PushTemplate, req.GetTxnMngrIP(), readRespPort)
	keyNode.pusherCache.Lock(keyNode.zmqInfo.context, addr)
	pusher := keyNode.pusherCache.GetSocket(addr)
	pusher.SendBytes(data, zmq.DONTWAIT)
	keyNode.pusherCache.Unlock(addr)
}

func validateHandler(keyNode *KeyNode, req *pb.ValidateRequest) {
	start := time.Now()
	action := keyNode.validate(req.GetTid(), req.GetBeginTS(), req.GetCommitTS(), req.GetKeys())
	end := time.Now()

	go keyNode.monitor.TrackStat(req.GetTid(), "Validation Time", end.Sub(start))

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
		Error:     pb.KeyError_K_SUCCESS,
		ChannelID: req.GetChannelID(),
	}
	data, _ := proto.Marshal(resp)

	addr := fmt.Sprintf(PushTemplate, req.GetTxnMngrIP(), valRespPort)
	keyNode.pusherCache.Lock(keyNode.zmqInfo.context, addr)
	pusher := keyNode.pusherCache.GetSocket(addr)
	pusher.SendBytes(data, zmq.DONTWAIT)
	keyNode.pusherCache.Unlock(addr)
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

	start := time.Now()
	err := keyNode.endTransaction(req.GetTid(), action, writeMap)
	end := time.Now()

	go keyNode.monitor.TrackStat(req.GetTid(), "End Transaction Time", end.Sub(start))

	var e pb.KeyError
	if err != nil {
		e = pb.KeyError_K_FAILURE
	} else {
		e = pb.KeyError_K_SUCCESS
	}

	resp := &pb.FinishResponse{
		Tid:       req.GetTid(),
		Error:     e,
		ChannelID: req.GetChannelID(),
	}

	data, _ := proto.Marshal(resp)

	addr := fmt.Sprintf(PushTemplate, req.GetTxnMngrIP(), endTxnRespPort)
	keyNode.pusherCache.Lock(keyNode.zmqInfo.context, addr)
	pusher := keyNode.pusherCache.GetSocket(addr)
	pusher.SendBytes(data, zmq.DONTWAIT)
	keyNode.pusherCache.Unlock(addr)
}

func NewKeyNode(debugMode bool) (*KeyNode, error) {
	configValue := config.ParseConfig()
	var storageManager storage.StorageManager
	switch configValue.StorageType {
	case "dynamo":
		storageManager = storage.NewDynamoStorageManager("Aftsi", "Aftsi")
	case "local":
		storageManager = storage.NewLocalStoreManager()
	case "anna":
		storageManager = storage.NewAnnaStorageManager(configValue.IpAddress, configValue.AnnaELB)
	default:
		log.Fatal(fmt.Sprintf("Unrecognized storageType %s. Valid types are: s3, dynamo, redis.", configValue.StorageType))
		os.Exit(3)
	}

	zctx, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}

	readPuller := cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, readPullPort), true)
	validatePuller := cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, validatePullPort), true)
	endTxnPuller := cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, endTxnPort), true)

	zmqInfo := ZMQInfo{
		context:        zctx,
		readPuller:     readPuller,
		validatePuller: validatePuller,
		endTxnPuller:   endTxnPuller,
	}

	pusherCache := cmn.NewSocketCache()

	var file *os.File
	if !debugMode {
		if _, err := os.Stat("logs"); os.IsNotExist(err) {
			os.Mkdir("logs", os.ModePerm)
		}
		file, err = os.OpenFile("logs/key-node", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
		log.SetOutput(file)
	}
	log.SetLevel(log.DebugLevel)

	monitor, err := cmn.NewStatsMonitor(mpb.NodeType_KEYNODE, configValue.IpAddress, configValue.MonitorIP)
	if err != nil {
		log.Error("Unable to create statistics monitor")
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
		zmqInfo:                    zmqInfo,
		pusherCache:                pusherCache,
		logFile:                    file,
		monitor:                    monitor,
	}, nil
}
