package routing

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	cmn "github.com/saurav-c/tasc/lib/common"
	annapb "github.com/saurav-c/tasc/proto/anna"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"os"
)

const (
	AnnaRouterPort = 6450
)

type AnnaRoutingClient struct {
	responseAddress string
	pusher0         *zmq.Socket
	pusher1         *zmq.Socket
	pusher2         *zmq.Socket
	pusher3         *zmq.Socket
}

func NewAnnaRoutingClient(elbAddress string, ipAddress string) *AnnaRoutingClient {
	context, err := zmq.NewContext()

	if err != nil {
		fmt.Println("Unexpected error while creating ZeroMQ context. Exiting:\n", err)
		os.Exit(1)
	}

	socket0 := cmn.CreateSocket(zmq.PUSH, context, fmt.Sprintf(cmn.PushTemplate, elbAddress, AnnaRouterPort), false)
	socket1 := cmn.CreateSocket(zmq.PUSH, context, fmt.Sprintf(cmn.PushTemplate, elbAddress, AnnaRouterPort+1), false)
	socket2 := cmn.CreateSocket(zmq.PUSH, context, fmt.Sprintf(cmn.PushTemplate, elbAddress, AnnaRouterPort+2), false)
	socket3 := cmn.CreateSocket(zmq.PUSH, context, fmt.Sprintf(cmn.PushTemplate, elbAddress, AnnaRouterPort+3), false)

	respAddr := fmt.Sprintf(cmn.PushTemplate, ipAddress, cmn.TxnRoutingPullPort)

	return &AnnaRoutingClient{
		responseAddress: respAddr,
		pusher0:         socket0,
		pusher1:         socket1,
		pusher2:         socket2,
		pusher3:         socket3,
	}
}

func (annaRtr *AnnaRoutingClient) lookup(tid string, keys []string) {
	request := &annapb.KeyAddressRequest{
		ResponseAddress: annaRtr.responseAddress,
		Keys:            keys,
		RequestId:       tid,
	}

	// Get random socket
	r := rand.Intn(4)
	var socket *zmq.Socket
	switch r {
	case 0:
		socket = annaRtr.pusher0
	case 1:
		socket = annaRtr.pusher1
	case 2:
		socket = annaRtr.pusher2
	case 3:
		socket = annaRtr.pusher3
	default:
		log.Debug("None of the routing cases applied...exiting")
		os.Exit(1)
	}
	bts, _ := proto.Marshal(request)
	socket.SendBytes(bts, zmq.DONTWAIT)
	log.Debug("Sent lookup request")
}
