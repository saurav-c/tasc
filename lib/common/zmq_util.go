package common

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"os"
	"sync"
)

const (
	PullTemplate = "tcp://*:%d"
	PushTemplate = "tcp://%s:%d"
)

type SocketCache struct {
	locks       map[string]*sync.Mutex
	sockets     map[string]*zmq.Socket
	lockMutex   *sync.RWMutex
	socketMutex *sync.RWMutex
}

func NewSocketCache() *SocketCache {
	return &SocketCache{
		locks:       make(map[string]*sync.Mutex),
		sockets:     make(map[string]*zmq.Socket),
		lockMutex:   &sync.RWMutex{},
		socketMutex: &sync.RWMutex{},
	}
}

func CreateSocket(tp zmq.Type, context *zmq.Context, address string, bind bool) *zmq.Socket {
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

func (cache *SocketCache) Lock(ctx *zmq.Context, address string) {
	cache.lockMutex.Lock()
	_, ok := cache.locks[address]
	if !ok {
		cache.socketMutex.Lock()
		cache.locks[address] = &sync.Mutex{}
		cache.sockets[address] = CreateSocket(zmq.PUSH, ctx, address, false)
		cache.socketMutex.Unlock()
	}
	addrLock := cache.locks[address]
	cache.lockMutex.Unlock()
	addrLock.Lock()
}

func (cache *SocketCache) Unlock(address string) {
	cache.lockMutex.RLock()
	cache.locks[address].Unlock()
	cache.lockMutex.RUnlock()
}

// Lock and Unlock need to be called on the Cache for this address
func (cache *SocketCache) GetSocket(address string) *zmq.Socket {
	cache.socketMutex.RLock()
	socket := cache.sockets[address]
	cache.socketMutex.RUnlock()
	return socket
}