package main

import (
	"fmt"
	uuid "github.com/nu7hatch/gouuid"
	zmq "github.com/pebbe/zmq4"
	"github.com/saurav-c/tasc/config"
	cmn "github.com/saurav-c/tasc/lib/common"
	"github.com/saurav-c/tasc/lib/routing"
	"github.com/saurav-c/tasc/lib/storage"
	"github.com/saurav-c/tasc/lib/worker"
	key "github.com/saurav-c/tasc/proto/keynode"
	mpb "github.com/saurav-c/tasc/proto/monitor"
	tpb "github.com/saurav-c/tasc/proto/tasc"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
)

type TransactionStatus uint

const (
	// Transaction Status
	Running TransactionStatus = iota
	Committing
	Aborting
	Complete
)

type TxnManager struct {
	Id               string
	ThreadId         int
	IpAddress        string
	PublicIP         string
	TransactionTable *TransactionTable
	WriteBuffer      *WriteBuffer
	StorageManager   storage.StorageManager
	ZmqInfo          *ZMQInfo
	PusherCache      *cmn.SocketCache
	LogFile          *os.File
	Monitor          *cmn.StatsMonitor
	RouterManager    routing.RouterManager
	WorkerConn       *worker.WorkerConn
	GC               *GarbageCollector
}

type TransactionTable struct {
	table map[string]*TransactionTableEntry
	mutex *sync.RWMutex
}

type TransactionTableEntry struct {
	beginTs      int64
	endTs        int64
	readSet      map[string]string
	coWrittenSet map[string]string
	status       TransactionStatus
	readChan     chan *key.KeyNodeResponse
	valChan      chan *key.ValidateResponse
	endTxnChan   chan *tpb.TransactionTag
	rtrChan      chan *routing.RoutingResponse
}

type GarbageCollector struct {
	mutex        *sync.RWMutex
	finishedTxns []string
}

type WriteBuffer struct {
	buffer map[string]*WriteBufferEntry
	mutex  *sync.RWMutex
}

type WriteBufferEntry struct {
	buffer map[string][]byte
}

type ZMQInfo struct {
	context      *zmq.Context
	readPuller   *zmq.Socket
	valPuller    *zmq.Socket
	endTxnPuller *zmq.Socket
	rtrPuller    *zmq.Socket
}

func NewTransactionManager(threadId int) (*TxnManager, error) {
	zctx, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}
	configValue := config.ParseConfig()
	var storageManager storage.StorageManager
	switch configValue.StorageType {
	case "dynamo":
		storageManager = storage.NewDynamoStorageManager("Aftsi", "Aftsi")
	case "local":
		storageManager = storage.NewLocalStoreManager()
	case "anna":
		storageManager = storage.NewAnnaStorageManager(configValue.PublicIP, configValue.AnnaELB)
	default:
		log.Fatal(fmt.Sprintf("Unrecognized storageType %s. Valid types are: anna, local, dynamo", configValue.StorageType))
	}

	zmqInfo := ZMQInfo{
		context:      zctx,
		readPuller:   cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(cmn.PullTemplate, cmn.TxnReadPullPort), true),
		valPuller:    cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(cmn.PullTemplate, cmn.TxnValidatePullPort), true),
		endTxnPuller: cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(cmn.PullTemplate, cmn.TxnAckPullPort), true),
		rtrPuller:    cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(cmn.PullTemplate, cmn.TxnRoutingPullPort), true),
	}

	// Create ZMQ Socket cache
	pusherCache := cmn.NewSocketCache()

	// Initialize logging
	logFile := cmn.InitLogger("logs", fmt.Sprintf(cmn.TxnLogTemplate, configValue.IpAddress, threadId), log.DebugLevel)

	// Create statistics monitor
	monitor, err := cmn.NewStatsMonitor(mpb.NodeType_TXNMANAGER, configValue.IpAddress, configValue.MonitorIP)
	if err != nil {
		log.Error("Unable to create statistics monitor")
	}

	// Generate unique node ID
	id, err := uuid.NewV4()
	if err != nil {
		log.Fatal("Unexpected error while generating UUID: %v", err)
		os.Exit(1)
	}

	table := &TransactionTable{
		table: make(map[string]*TransactionTableEntry),
		mutex: &sync.RWMutex{},
	}

	buffer := &WriteBuffer{
		buffer: make(map[string]*WriteBufferEntry),
		mutex:  &sync.RWMutex{},
	}

	gc := &GarbageCollector{
		mutex:        &sync.RWMutex{},
		finishedTxns: make([]string, 100),
	}

	rtrManager := routing.NewAnnaRoutingManager(configValue.IpAddress, configValue.RoutingILB)
	wcn := worker.NewWorkerConn(configValue.IpAddress, configValue.WorkerILB)

	return &TxnManager{
		Id:               id.String(),
		ThreadId:         threadId,
		IpAddress:        configValue.IpAddress,
		PublicIP:         configValue.PublicIP,
		TransactionTable: table,
		WriteBuffer:      buffer,
		StorageManager:   storageManager,
		ZmqInfo:          &zmqInfo,
		PusherCache:      pusherCache,
		RouterManager:    rtrManager,
		LogFile:          logFile,
		Monitor:          monitor,
		WorkerConn:       wcn,
		GC:               gc,
	}, nil
}
