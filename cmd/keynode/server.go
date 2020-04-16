package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	storage "github.com/saurav-c/aftsi/lib/storage"
)

const (
	ReadCacheLimit = 1000
)

func findIndex (a []string, e string) (int) {
	for index, elem := range a {
		if elem == e {
			return index
		}
	}
	return -1
}

func intersection (a []string, b []string) (bool) {
	var hash map[string]bool
	for _, elem := range a {
		hash[elem] = true
	}
	for _, elem := range b {
		if hash[elem] {
			return true
		}
	}
	return false
}

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
	StorageManager              storage.StorageManager
	keyVersionIndex             map[string][]*keyVersion
	keyVersionIndexLock         map[string]*sync.RWMutex
	pendingKeyVersionIndex      map[string][]*keyVersion
	pendingKeyVersionIndexLock  map[string]*sync.RWMutex
	pendingTxnCache             map[string]*pendingTxn
	committedTxnCache           map[string][]string
	readCache                   map[string][]byte
	readCacheLock               *sync.RWMutex
}

func NewKeyNode(KeyNodeIP string, storageType string) (*KeyNode, int, error){
	// TODO: Integrate this into config manager
	// Need to change parameters to fit around needs better
	var storageManager storage.StorageManager
	switch storageType {
	case "dynamo":
		storageManager = storage.NewDynamoStorageManager("AftSiData", "AftSiData")
	default:
		log.Fatal(fmt.Sprintf("Unrecognized storageType %s. Valid types are: s3, dynamo, redis.", conf.StorageType))
		os.Exit(3)
	}

	// TODO: Need to create ZMQ Connections
	return &KeyNode{
		StorageManager:             storageManager,
		keyVersionIndex:           	make(map[string][]*keyVersion),
		keyVersionIndexLock:        make(map[string]*sync.RWMutex),
		pendingKeyVersionIndex:     make(map[string][]*keyVersion),
		pendingKeyVersionIndexLock: make(map[string]*sync.RWMutex),
		committedTxnCache:          make(map[string][]string),
		readCache:                  make(map[string][]byte),
		readCacheLock:              &sync.RWMutex{},
	}, 0, nil
}
