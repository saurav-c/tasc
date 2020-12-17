package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"github.com/saurav-c/aftsi/config"
	cmn "github.com/saurav-c/aftsi/lib/common"
	"github.com/saurav-c/aftsi/lib/storage"
	mpb "github.com/saurav-c/aftsi/proto/monitor"
	log "github.com/sirupsen/logrus"
	"os"
)

type KeyNode struct {
	IpAddress             string
	StorageManager        storage.StorageManager
	CommittedVersionIndex *VersionIndex
	PendingVersionIndex   *VersionIndex
	CommittedTxnSet       *TransactionSet
	PendingTxnSet         *TransactionSet
	ZmqInfo               ZMQInfo
	PusherCache           *cmn.SocketCache
	LogFile               *os.File
	Monitor               *cmn.StatsMonitor
}

type ZMQInfo struct {
	context        *zmq.Context
	readPuller     *zmq.Socket
	validatePuller *zmq.Socket
	endTxnPuller   *zmq.Socket
}

func NewKeyNode() (*KeyNode, error) {
	configValue := config.ParseConfig()
	var storageManager storage.StorageManager
	switch configValue.StorageType {
	case "dynamo":
		storageManager = storage.NewDynamoStorageManager("Aftsi", "Aftsi")
	case "local":
		storageManager = storage.NewLocalStoreManager()
	//case "anna":
	//	storageManager = storage.NewAnnaStorageManager(configValue.IpAddress, configValue.AnnaELB)
	default:
		log.Fatal(fmt.Sprintf("Unrecognized storageType %s. Valid types are: s3, dynamo, redis.", configValue.StorageType))
		os.Exit(3)
	}

	zctx, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}

	readPuller := cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(cmn.PullTemplate, cmn.KeyReadPullPort), true)
	validatePuller := cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(cmn.PullTemplate, cmn.KeyValidatePullPort), true)
	endTxnPuller := cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(cmn.PullTemplate, cmn.KeyEndTxnPullPort), true)

	zmqInfo := ZMQInfo{
		context:        zctx,
		readPuller:     readPuller,
		validatePuller: validatePuller,
		endTxnPuller:   endTxnPuller,
	}

	pusherCache := cmn.NewSocketCache()

	file := cmn.InitLogger("logs", fmt.Sprintf(cmn.KeyLogTemplate, configValue.IpAddress, 0), log.DebugLevel)

	monitor, err := cmn.NewStatsMonitor(mpb.NodeType_KEYNODE, configValue.IpAddress, configValue.MonitorIP)
	if err != nil {
		log.Error("Unable to create statistics Monitor")
	}

	committedIndex := NewVersionIndex()
	pendingIndex := NewVersionIndex()
	committedTxnSet := NewTransactionSet()
	pendingTxnSet := NewTransactionSet()

	return &KeyNode{
		IpAddress:             configValue.IpAddress,
		StorageManager:        storageManager,
		CommittedVersionIndex: &committedIndex,
		PendingVersionIndex:   &pendingIndex,
		CommittedTxnSet:       &committedTxnSet,
		PendingTxnSet:         &pendingTxnSet,
		ZmqInfo:               zmqInfo,
		PusherCache:           pusherCache,
		LogFile:               file,
		Monitor:               monitor,
	}, nil
}
