package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"github.com/saurav-c/aftsi/config"
	"github.com/saurav-c/aftsi/lib/storage"
	pb "github.com/saurav-c/aftsi/proto/keynode/api"
	mt "github.com/saurav-c/aftsi/proto/monitor"
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

	// Monitor Node Port
	monitorPushPort = 10000
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
	logFile                    *os.File
	monitor                    *Monitor
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

type Monitor struct {
	pusher   *zmq.Socket
	statsMap map[string][]float64
	lock     *sync.Mutex
}

func (monitor *Monitor) trackStat(msg string, latency float64) {
	msg = "KEYNODE" + "-" + msg
	monitor.lock.Lock()
	if _, ok := monitor.statsMap[msg]; !ok {
		monitor.statsMap[msg] = make([]float64, 0)
	}
	monitor.statsMap[msg] = append(monitor.statsMap[msg], latency)
	monitor.lock.Unlock()
}

func (monitor *Monitor) sendStats() {
	for true {
		log.Debug("Triggered sendStats")
		time.Sleep(100 * time.Millisecond)
		statsMap := make(map[string]*mt.Latencies)
		monitor.lock.Lock()
		if len(monitor.statsMap) == 0 {
			monitor.lock.Unlock()
			continue
		}
		for msg, lats := range monitor.statsMap {
			latencies := &mt.Latencies{
				Value: lats,
			}
			statsMap[msg] = latencies
		}
		monitor.statsMap = make(map[string][]float64)
		monitor.lock.Unlock()
		statsMsg := &mt.Statistics{
			Stats: statsMap,
		}
		data, _ := proto.Marshal(statsMsg)
		monitor.pusher.SendBytes(data, zmq.DONTWAIT)
		log.Debug("Sent stats to monitor")
	}
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

func (k *KeyNode) logExecutionTime(msg string, diff time.Duration) {
	log.Debugf("%s: %f ms", msg, diff.Seconds() * 1000)
	k.monitor.trackStat(msg, diff.Seconds() * 1000)
}

func startKeyNode(keyNode *KeyNode) {
	go keyNode.monitor.sendStats()

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
	go keyNode.logExecutionTime("Read Key Time", end.Sub(start))

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
	keyNode.pusherCache.lock(keyNode.zmqInfo.context, addr)
	pusher := keyNode.pusherCache.getSocket(addr)
	pusher.SendBytes(data, zmq.DONTWAIT)
	keyNode.pusherCache.unlock(addr)
}

func validateHandler(keyNode *KeyNode, req *pb.ValidateRequest) {
	start := time.Now()
	action := keyNode.validate(req.GetTid(), req.GetBeginTS(), req.GetCommitTS(), req.GetKeys())
	end := time.Now()
	go keyNode.logExecutionTime("Validation Time", end.Sub(start))

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

	start := time.Now()
	err := keyNode.endTransaction(req.GetTid(), action, writeMap)
	end := time.Now()
	go keyNode.logExecutionTime("End Transaction Time", end.Sub(start))

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
	keyNode.pusherCache.lock(keyNode.zmqInfo.context, addr)
	pusher := keyNode.pusherCache.getSocket(addr)
	pusher.SendBytes(data, zmq.DONTWAIT)
	keyNode.pusherCache.unlock(addr)
}

func NewKeyNode(debugMode bool) (*KeyNode, error) {
	// TODO: Integrate this into config manager
	// Need to change parameters to fit around needs better
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

	monitorPusher := createSocket(zmq.PUSH, zctx, fmt.Sprintf(PushTemplate, configValue.MonitorIP, monitorPushPort), false)
	monitor := Monitor{
		pusher:   monitorPusher,
		statsMap: make(map[string][]float64),
		lock:     &sync.Mutex{},
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
		batchMode:                  configValue.Batch,
		logFile:                    file,
		monitor:                    &monitor,
	}, nil
}
