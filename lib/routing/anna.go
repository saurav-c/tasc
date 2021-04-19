package routing

import "sync"

type AnnaRoutingManager struct {
	freeClients []*AnnaRoutingClient
	cond        *sync.Cond
}

func NewAnnaRoutingManager(ipAddress string, elbAddress string) *AnnaRoutingManager {
	clients := []*AnnaRoutingClient{}
	for i := 0; i < 10; i++ {
		annaRtr := NewAnnaRoutingClient(elbAddress, ipAddress)
		clients = append(clients, annaRtr)
	}

	return &AnnaRoutingManager{
		freeClients: clients,
		cond:        sync.NewCond(&sync.Mutex{}),
	}
}

func (annaRtr *AnnaRoutingManager) Lookup(tid string, keys []string) error {
	client := annaRtr.getClient()
	defer annaRtr.releaseClient(client)
	client.lookup(tid, keys)
	return nil
}

func (annaRtr *AnnaRoutingManager) getClient() *AnnaRoutingClient {
	annaRtr.cond.L.Lock()
	for len(annaRtr.freeClients) == 0 {
		annaRtr.cond.Wait()
	}
	client := annaRtr.freeClients[0]
	annaRtr.freeClients = annaRtr.freeClients[1:]
	annaRtr.cond.L.Unlock()

	return client
}

func (annaRtr *AnnaRoutingManager) releaseClient(client *AnnaRoutingClient) {
	annaRtr.cond.L.Lock()
	annaRtr.freeClients = append(annaRtr.freeClients, client)
	annaRtr.cond.Signal()
	annaRtr.cond.L.Unlock()
}
