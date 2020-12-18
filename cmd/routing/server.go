package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"github.com/saurav-c/tasc/config"
	cmn "github.com/saurav-c/tasc/lib/common"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

type RouterServer struct {
	NodeIPs     []string
	ZmqInfo     *ZMQInfo
	PusherCache *cmn.SocketCache
	LogFile     *os.File
}

type ZMQInfo struct {
	context *zmq.Context
	puller  *zmq.Socket
}

func (r *RouterServer) listener() {
	poller := zmq.NewPoller()
	info := r.ZmqInfo
	poller.Add(info.puller, zmq.POLLIN)

	for true {
		sockets, _ := poller.Poll(10 * time.Millisecond)

		for _, socket := range sockets {
			switch s := socket.Socket; s {
			case info.puller:
				{
					data, _ := info.puller.RecvBytes(zmq.DONTWAIT)
					go r.lookupHandler(data)
				}
			}
		}
	}
}

func NewRouter() (*RouterServer, error) {
	configValue := config.ParseConfig()
	IpAddresses := configValue.NodeIPs

	zctx, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}
	zmqInfo := &ZMQInfo{
		context:      zctx,
		puller:    cmn.CreateSocket(zmq.PULL, zctx, fmt.Sprintf(cmn.PullTemplate, cmn.RouterPullPort), true),
	}
	pusherCache := cmn.NewSocketCache()
	file := cmn.InitLogger("logs", "router", log.DebugLevel)

	return &RouterServer{
		NodeIPs:     IpAddresses,
		ZmqInfo:     zmqInfo,
		PusherCache: pusherCache,
		LogFile:     file,
	}, nil
}
