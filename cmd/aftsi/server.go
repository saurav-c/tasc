package main

import (
  "sync"
  "time"
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
	IpAddress            string
	StorageManager       storage.StorageManager
	ConsistencyManager   consistency.ConsistencyManager
	TransactionTable     map[string]*TransactionEntry
	TransactionTableLock *sync.RWMutex
	WriteBuffer          map[string][]*keyUpdate
	WriteBufferLock      *sync.RWMutex
	ReadCache            map[string]pb.KeyValuePair
	ReadCacheLock        *sync.RWMutex
}

func NewAftSIServer() {
  panic("implement me!")
}
