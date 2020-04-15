package main

import "sync"

func InsertParticularIndex(list []*keyVersion, kv *keyVersion) []*keyVersion {
	if len(list) == 0 {
		return []*keyVersion{kv}
	}
	index := FindIndex(list, kv)
	return append(append(list[:index], kv), list[index:]...)
}

func FindIndex(list []*keyVersion, kv *keyVersion) int {
	startList := 0
	endList := len(list) - 1
	midPoint := 0
	for true {
		midPoint = (startList + endList) / 2
		midElement := list[midPoint]
		if midElement.CommitTS == kv.CommitTS {
			return midPoint
		} else if kv.CommitTS < list[startList].CommitTS {
			return startList
		} else if kv.CommitTS > list[endList].CommitTS {
			return endList
		} else if midElement.CommitTS > kv.CommitTS {
			startList = midPoint
		} else {
			endList = midPoint
		}
	}
}

type keyVersion struct {
	tid      string
	CommitTS string
}

type pendingTxn struct {
	keys []string
	commitTS string
}

type KeyNode struct {
	keyVersionIndex             map[string][]*keyVersion
	keyVersionIndexLock         map[string]*sync.RWMutex
	pendingKeyVersionIndex      map[string][]*keyVersion
	pendingKeyVersionIndexLock  map[string]*sync.RWMutex
	pendingTxnCache             map[string]*pendingTxn
	committedTxnCache           map[string][]string
	commitedTxnCacheLock        map[string]*sync.RWMutex
	readCache                   map[string][]byte
	readCacheLock               *sync.RWMutex
}

func NewKeyNode(KeyNodeIP string) (*KeyNode, int, error){
	// TODO: Need to create ZMQ Connections
	return &KeyNode{
		keyVersionIndex:           	make(map[string][]*keyVersion),
		keyVersionIndexLock:        make(map[string]*sync.RWMutex),
		pendingKeyVersionIndex:     make(map[string][]*keyVersion),
		pendingKeyVersionIndexLock: make(map[string]*sync.RWMutex),
		committedTxnCache:          make(map[string][]string),
		commitedTxnCacheLock:       make(map[string]*sync.RWMutex),
		readCache:                  make(map[string][]byte),
		readCacheLock:              &sync.RWMutex{},
	}, 0, nil
}
