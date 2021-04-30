package main

import (
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	mt "github.com/saurav-c/tasc/proto/monitor"
	cmn "github.com/saurav-c/tasc/lib/common"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

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

func main() {
	zctx, _ := zmq.NewContext()
	puller := createSocket(zmq.PULL, zctx, fmt.Sprintf(cmn.PullTemplate, cmn.MonitorPushPort), true)
	poller := zmq.NewPoller()
	poller.Add(puller, zmq.POLLIN)

	if _, err := os.Stat("logs"); os.IsNotExist(err) {
		os.Mkdir("logs", os.ModePerm)
	}
	file, err := os.OpenFile("logs/monitor", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)

	if _, err := os.Stat("stats"); os.IsNotExist(err) {
		os.Mkdir("stats", os.ModePerm)
	}

	log.Info("Started monitoring node")

	for true {
		sockets, _ := poller.Poll(10 * time.Millisecond)

		for _, socket := range sockets {
			switch s := socket.Socket; s {
			case puller:
				{
					data, _ := puller.RecvBytes(zmq.DONTWAIT)
					logStatistics(data)
				}
			}
		}
	}
}

func logStatistics(data []byte) {
	log.Debug("Received statistics message")
	statResp := &mt.Statistics{}
	proto.Unmarshal(data, statResp)

	statsMap := statResp.GetStats()
	nodeType := statResp.GetNode()
	nodeAddr := statResp.GetAddr()

	jdoc, err := json.Marshal(statsMap)
	if err != nil {
		log.Fatal(err)
	}
	jstring := string(jdoc)

	fileName := "stats/"
	if nodeType == mt.NodeType_TXNMANAGER {
		fileName += "txn-manager_" + nodeAddr
	} else if nodeType == mt.NodeType_KEYNODE {
		fileName += "key-node_" + nodeAddr
	} else if nodeType == mt.NodeType_WORKER {
		fileName += "worker_" + nodeAddr
	} else {
		log.Errorf("Unknown node type %s from node %s", nodeType, nodeAddr)
		return
	}

	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		jstring = "[" + jstring
	}

	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	f.WriteString(jstring + ", ")
	f.Close()
}
