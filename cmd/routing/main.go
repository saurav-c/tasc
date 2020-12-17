package main

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	cmn "github.com/saurav-c/aftsi/lib/common"
	rpb "github.com/saurav-c/aftsi/proto/router"
	log "github.com/sirupsen/logrus"
	"time"
)

func (r *RouterServer) lookup(key string) string {
	h := sha1.New()
	keySha := h.Sum([]byte(key))
	intSha := binary.BigEndian.Uint64(keySha)
	index := intSha % uint64(len(r.NodeIPs))
	ipAddress := r.NodeIPs[index]
	return ipAddress
}

func (r *RouterServer) lookupHandler(data []byte) {
	log.Debug("Received lookup request")
	request := &rpb.RouterRequest{}
	proto.Unmarshal(data, request)
	keyMap := map[string]string{}
	for _, key := range request.Keys {
		keyMap[key] = r.lookup(key)
	}

	response := &rpb.RouterResponse{
		Tid:        request.Tid,
		AddressMap: keyMap,
	}
	respData, _ := proto.Marshal(response)

	addr := fmt.Sprintf(cmn.PushTemplate, request.IpAddress, cmn.TxnRoutingPullPort)
	log.Debugf("Sending routing response to %s", addr)
	r.PusherCache.Lock(r.ZmqInfo.context, addr)
	socket := r.PusherCache.GetSocket(addr)
	socket.SendBytes(respData, zmq.DONTWAIT)
	r.PusherCache.Unlock(addr)
	log.Debug("Sent routing response")
}

func main() {
	router, err := NewRouter()
	if err != nil {
		log.Fatal("Could not start router: %v\n", err)
	}
	log.Infof("Starting router at %s.\n", time.Now().String())
	router.listener()
}
