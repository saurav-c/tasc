package main

import (
	"fmt"
	uuid "github.com/nu7hatch/gouuid"
	zmq "github.com/pebbe/zmq4"
	"github.com/saurav-c/tasc/config"
	cmn "github.com/saurav-c/tasc/lib/common"
	"github.com/saurav-c/tasc/lib/routing"
	"log"
	"os"
	"sync"
)

type TxnWorker struct {
	Id            string
	IpAddress     string
	PublicIP      string
	RouterManager routing.RouterManager
	ZMQInfo       *ZMQInfo
	PusherCache   *cmn.SocketCache
	RtrChanMap    map[string]chan *routing.RoutingResponse
	RtrChanMutex  *sync.RWMutex
}

type ZMQInfo struct {
	context   *zmq.Context
	txnPuller *zmq.Socket
	rtrPuller *zmq.Socket
}

func NewTransactionWorker() (*TxnWorker, error) {
	zctx, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}
	configValue := config.ParseConfig()

	zmqInfo := &ZMQInfo{
		context:   zctx,
		txnPuller: cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(cmn.PullTemplate, cmn.WorkerPullPort), true),
		rtrPuller: cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(cmn.PullTemplate, cmn.WorkerRtrPullPort), true),
	}

	// Create ZMQ Socket cache
	pusherCache := cmn.NewSocketCache()

	// Generate unique worker ID
	id, err := uuid.NewV4()
	if err != nil {
		log.Fatal("Unexpected error while generating UUID: %v", err)
		os.Exit(1)
	}

	rtrManager := routing.NewAnnaRoutingManager(configValue.IpAddress, configValue.RoutingILB)

	return &TxnWorker{
		Id:            "worker-" + id.String(),
		IpAddress:     configValue.IpAddress,
		PublicIP:      configValue.PublicIP,
		RouterManager: rtrManager,
		ZMQInfo:       zmqInfo,
		PusherCache:   pusherCache,
		RtrChanMap:    make(map[string]chan *routing.RoutingResponse),
		RtrChanMutex:  &sync.RWMutex{},
	}, nil
}
