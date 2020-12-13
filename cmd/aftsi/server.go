package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/saurav-c/aftsi/config"
	pb "github.com/saurav-c/aftsi/proto/aftsi/api"
	mt "github.com/saurav-c/aftsi/proto/monitor/api"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"github.com/saurav-c/aftsi/lib/storage"
	key "github.com/saurav-c/aftsi/proto/keynode/api"
	rtr "github.com/saurav-c/aftsi/proto/routing/api"
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

	// In ms
	FlushFrequency = 1000

	// Monitor Node Port
	monitorPushPort = 10000
)

type TransactionEntry struct {
	beginTS      string
	endTS        string
	readSet      map[string]string
	coWrittenSet map[string]string
	status       uint8
}

type WriteBufferEntry struct {
	buffer map[string][]byte
}

type AftSIServer struct {
	counter          uint64
	counterMutex     *sync.Mutex
	IPAddress        string
	serverID         string
	keyRouterConn    rtr.RouterClient
	StorageManager   storage.StorageManager
	TransactionTable map[string]*TransactionEntry
	TransactionMutex *sync.RWMutex
	WriteBuffer      map[string]*WriteBufferEntry
	WriteBufferMutex *sync.RWMutex
	ReadCache        map[string][]byte
	ReadCacheLock    *sync.RWMutex
	commitBuffer     map[string][]byte
	commitLock       *sync.Mutex
	zmqInfo          ZMQInfo
	Responder        *ResponseHandler
	PusherCache      *SocketCache
	logFile          *os.File
	monitor          *Monitor
}

type ZMQInfo struct {
	context             *zmq.Context
	createTxnReqPuller  *zmq.Socket
	createTxnRespPuller *zmq.Socket
	readPuller          *zmq.Socket
	valPuller           *zmq.Socket
	endTxnPuller        *zmq.Socket
}

type ResponseHandler struct {
	readChannels      map[uint32](chan *key.KeyNodeResponse)
	readMutex         *sync.RWMutex
	validateChannels  map[uint32](chan *key.ValidateResponse)
	valMutex          *sync.RWMutex
	endTxnChannels    map[uint32](chan *key.FinishResponse)
	endMutex          *sync.RWMutex
	createTxnChannels map[uint32](chan *pb.CreateTxnEntryResp)
	createMutex       *sync.RWMutex
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
	msg = "TXNMANAGER" + "-" + msg
	monitor.lock.Lock()
	if _, ok := monitor.statsMap[msg]; !ok {
		monitor.statsMap[msg] = make([]float64, 0)
	}
	monitor.statsMap[msg] = append(monitor.statsMap[msg], latency)
	monitor.lock.Unlock()
}

func (monitor *Monitor) sendStats() {
	for true {
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

func flusher(s *AftSIServer) {
	for {
		if len(s.commitBuffer) > 0 {
			e := s._flushBuffer()
			if e != nil {
				fmt.Println(e.Error())
			}
		}
		time.Sleep(FlushFrequency * time.Millisecond)
	}
}

// Listens for incoming requests via ZMQ
func txnManagerListen(server *AftSIServer) {
	// Create a new poller to wait for new updates
	poller := zmq.NewPoller()

	info := server.zmqInfo

	poller.Add(info.createTxnReqPuller, zmq.POLLIN)
	poller.Add(info.createTxnRespPuller, zmq.POLLIN)
	poller.Add(info.readPuller, zmq.POLLIN)
	poller.Add(info.valPuller, zmq.POLLIN)
	poller.Add(info.endTxnPuller, zmq.POLLIN)

	for true {
		sockets, _ := poller.Poll(10 * time.Millisecond)

		for _, socket := range sockets {
			switch s := socket.Socket; s {
			case info.createTxnRespPuller:
				{
					data, _ := info.createTxnRespPuller.RecvBytes(zmq.DONTWAIT)
					go createTxnRespHandler(data, server.Responder)
				}
			case info.readPuller:
				{
					data, _ := info.readPuller.RecvBytes(zmq.DONTWAIT)
					go readHandler(data, server.Responder)
				}
			case info.valPuller:
				{
					data, _ := info.valPuller.RecvBytes(zmq.DONTWAIT)
					go validateHandler(data, server.Responder)
				}
			case info.endTxnPuller:
				{
					data, _ := info.endTxnPuller.RecvBytes(zmq.DONTWAIT)
					go endTxnHandler(data, server.Responder)
				}
			}
		}
	}
}

// Create Txn Entry Response Handler
func createTxnRespHandler(data []byte, responder *ResponseHandler) {
	resp := &pb.CreateTxnEntryResp{}
	proto.Unmarshal(data, resp)
	channelID := resp.GetChannelID()
	responder.createMutex.RLock()
	channel := responder.createTxnChannels[channelID]
	responder.createMutex.RUnlock()
	channel <- resp
}

// Key Node Response Handlers

func readHandler(data []byte, responder *ResponseHandler) {
	resp := &key.KeyNodeResponse{}
	proto.Unmarshal(data, resp)
	channelID := resp.GetChannelID()
	responder.readMutex.RLock()
	channel := responder.readChannels[channelID]
	responder.readMutex.RUnlock()
	channel <- resp
}

func validateHandler(data []byte, responder *ResponseHandler) {
	resp := &key.ValidateResponse{}
	proto.Unmarshal(data, resp)
	channelID := resp.GetChannelID()
	responder.valMutex.RLock()
	channel := responder.validateChannels[channelID]
	responder.valMutex.RUnlock()
	channel <- resp
}

func endTxnHandler(data []byte, responder *ResponseHandler) {
	resp := &key.FinishResponse{}
	proto.Unmarshal(data, resp)
	channelID := resp.GetChannelID()
	responder.endMutex.RLock()
	channel := responder.endTxnChannels[channelID]
	responder.endMutex.RUnlock()
	channel <- resp
}

func NewAftSIServer(debugMode bool) (*AftSIServer, int, error) {
	zctx, err := zmq.NewContext()
	if err != nil {
		return nil, 0, err
	}
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
		log.Fatal(fmt.Sprintf("Unrecognized storageType %s. Valid types are: anna, local, dynamo", configValue.StorageType))
		os.Exit(3)
	}

	keyRouterIP := configValue.KeyRouterIP

	connKey, err := grpc.Dial(keyRouterIP+":5007", grpc.WithInsecure())
	KeyRouterClient := rtr.NewRouterClient(connKey)

	// Setup Txn Manager ZMQ sockets
	createTxnReqPuller := createSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, createTxnPortReq), true)
	createTxnRespPuller := createSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, createTxnPortResp), true)

	// Setup Key Node sockets
	readPuller := createSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, ReadRespPullPort), true)
	validatePuller := createSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, ValRespPullPort), true)
	endTxnPuller := createSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, EndTxnRespPort), true)

	zmqInfo := ZMQInfo{
		context:             zctx,
		createTxnReqPuller:  createTxnReqPuller,
		createTxnRespPuller: createTxnRespPuller,
		readPuller:          readPuller,
		valPuller:           validatePuller,
		endTxnPuller:        endTxnPuller,
	}

	responder := ResponseHandler{
		readChannels:      make(map[uint32](chan *key.KeyNodeResponse)),
		validateChannels:  make(map[uint32](chan *key.ValidateResponse)),
		endTxnChannels:    make(map[uint32](chan *key.FinishResponse)),
		createTxnChannels: make(map[uint32](chan *pb.CreateTxnEntryResp)),
		readMutex:         &sync.RWMutex{},
		valMutex:          &sync.RWMutex{},
		endMutex:          &sync.RWMutex{},
		createMutex:       &sync.RWMutex{},
	}

	pusherCache := SocketCache{
		locks:       make(map[string]*sync.Mutex),
		sockets:     make(map[string]*zmq.Socket),
		lockMutex:   &sync.RWMutex{},
		socketMutex: &sync.RWMutex{},
	}

	serverID := "0"

	var file *os.File
	if !debugMode {
		if _, err := os.Stat("logs"); os.IsNotExist(err) {
			os.Mkdir("logs", os.ModePerm)
		}
		file, err = os.OpenFile("logs/txn-manager"+serverID, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
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

	return &AftSIServer{
		counter:          0,
		counterMutex:     &sync.Mutex{},
		IPAddress:        configValue.IpAddress,
		serverID:         serverID,
		keyRouterConn:    KeyRouterClient,
		StorageManager:   storageManager,
		commitBuffer:     make(map[string][]byte),
		commitLock:       &sync.Mutex{},
		TransactionTable: make(map[string]*TransactionEntry),
		TransactionMutex: &sync.RWMutex{},
		WriteBuffer:      make(map[string]*WriteBufferEntry),
		WriteBufferMutex: &sync.RWMutex{},
		ReadCache:        make(map[string][]byte),
		ReadCacheLock:    &sync.RWMutex{},
		zmqInfo:          zmqInfo,
		Responder:        &responder,
		PusherCache:      &pusherCache,
		logFile:          file,
		monitor:          &monitor,
	}, 0, nil
}
