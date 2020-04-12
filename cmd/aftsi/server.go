package main

import (
	"sync"
)

type TransactionEntry struct {
	begin_ts      string
	end_ts        string
	readSet       []string
	coWrittenSets map[string][]string
	status        int8
	opCounter     int64
}

type AftSIServer struct {
	counter   uint64 // import "sync/atomic" when using this
	counterMutex &sync.Mutex
	IpAddress    string
	serverID 	   string	
	StorageManager storage.StorageManager
	// ConsistencyManager   consistency.ConsistencyManager
	TransactionTable     map[string]*TransactionEntry
	TransactionTableLock *sync.RWMutex
	WriteBuffer          map[string]map[string]byte
	WriteBufferLock      *sync.RWMutex
	ReadCache            map[string]pb.KeyValuePair
	ReadCacheLock        *sync.RWMutex
}

func NewAftSIServer() {
	panic("implement me!")
}
