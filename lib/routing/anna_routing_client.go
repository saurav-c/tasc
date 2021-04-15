package routing

import (
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	cmn "github.com/saurav-c/tasc/lib/common"
	annapb "github.com/saurav-c/tasc/proto/anna"
	"golang.org/x/tools/go/ssa/interp/testdata/src/fmt"
	"math/rand"
	"os"
)

const (
	AnnaRouterPort = 6450
)

type AnnaRoutingClient struct {
	responseAddress string
	routerPushers   []*zmq.Socket
}

func NewAnnaRoutingClient(elbAddress string, ipAddress string) *AnnaRoutingClient {
	context, err := zmq.NewContext()

	if err != nil {
		fmt.Println("Unexpected error while creating ZeroMQ context. Exiting:\n", err)
		os.Exit(1)
	}

	pushers := make([]*zmq.Socket, 4)
	for i := 0; i < 4; i++ {
		lookupAddress := fmt.Sprintf(cmn.PushTemplate, elbAddress, AnnaRouterPort+i)
		socket := cmn.CreateSocket(zmq.PUSH, context, lookupAddress, false)
		pushers = append(pushers, socket)
	}

	respAddr := fmt.Sprintf(cmn.PushTemplate, ipAddress, cmn.TxnRoutingPullPort)

	return &AnnaRoutingClient{
		responseAddress:     respAddr,
		routerPushers: pushers,
	}
}

func (annaRtr *AnnaRoutingClient) lookup(tid string, keys []string) {
	request := &annapb.KeyAddressRequest{
		ResponseAddress: annaRtr.responseAddress,
		Keys:            keys,
		RequestId:       tid,
	}

	// Get random socket
	socket := annaRtr.routerPushers[rand.Intn(4)]

	bts, _ := proto.Marshal(request)
	socket.SendBytes(bts, zmq.DONTWAIT)
}
