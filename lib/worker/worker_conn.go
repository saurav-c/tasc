package worker

import (
	tpb "github.com/saurav-c/tasc/proto/tasc"
	"sync"
)

type WorkerConn struct {
	freeClients []*WorkerClient
	cond        *sync.Cond
}

func NewWorkerConn(ipAddress string, ilbAddress string) *WorkerConn {
	clients := []*WorkerClient{}
	for i := 0; i < 10; i++ {
		wc := NewWorkerClient(ilbAddress, ipAddress)
		clients = append(clients, wc)
	}

	return &WorkerConn{
		freeClients: clients,
		cond:        sync.NewCond(&sync.Mutex{}),
	}
}

func (wcn *WorkerConn) SendWork(tid string, status tpb.TascTransactionStatus, writeset []string) {
	client := wcn.getClient()
	defer wcn.releaseClient(client)
	client.sendWork(tid, status, writeset)
}

func (wcn *WorkerConn) getClient() *WorkerClient {
	wcn.cond.L.Lock()
	for len(wcn.freeClients) == 0 {
		wcn.cond.Wait()
	}
	client := wcn.freeClients[0]
	wcn.freeClients = wcn.freeClients[1:]
	wcn.cond.L.Unlock()
	return client
}

func (wcn *WorkerConn) releaseClient(client *WorkerClient) {
	wcn.cond.L.Lock()
	wcn.freeClients = append(wcn.freeClients, client)
	wcn.cond.Signal()
	wcn.cond.L.Unlock()
}
