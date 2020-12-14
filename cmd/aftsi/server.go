package main

import (
	"fmt"
	cmn "github.com/saurav-c/aftsi/lib/common"
	mpb "github.com/saurav-c/aftsi/proto/monitor"
	"os"
	"sync"
	"time"

	"github.com/saurav-c/aftsi/config"
	pb "github.com/saurav-c/aftsi/proto/aftsi"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"github.com/saurav-c/aftsi/lib/storage"
	key "github.com/saurav-c/aftsi/proto/keynode"
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
	PusherCache      *cmn.SocketCache
	logFile          *os.File
	monitor          *cmn.StatsMonitor
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
	createTxnReqPuller := cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, createTxnPortReq), true)
	createTxnRespPuller := cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, createTxnPortResp), true)

	// Setup Key Node sockets
	readPuller := cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, ReadRespPullPort), true)
	validatePuller := cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, ValRespPullPort), true)
	endTxnPuller := cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(PullTemplate, EndTxnRespPort), true)

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

	pusherCache := cmn.NewSocketCache()

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

	monitor, err := cmn.NewStatsMonitor(mpb.NodeType_TXNMANAGER, configValue.IpAddress, configValue.MonitorIP)
	if err != nil {
		log.Error("Unable to create statistics monitor")
	}

	return &AftSIServer{
		counter:          0,
		counterMutex:     &sync.Mutex{},
		IPAddress:        configValue.IpAddress,
		serverID:         serverID,
		keyRouterConn:    KeyRouterClient,
		StorageManager:   storageManager,
		TransactionTable: make(map[string]*TransactionEntry),
		TransactionMutex: &sync.RWMutex{},
		WriteBuffer:      make(map[string]*WriteBufferEntry),
		WriteBufferMutex: &sync.RWMutex{},
		ReadCache:        make(map[string][]byte),
		ReadCacheLock:    &sync.RWMutex{},
		zmqInfo:          zmqInfo,
		Responder:        &responder,
		PusherCache:      pusherCache,
		logFile:          file,
		monitor:          monitor,
	}, 0, nil
}
