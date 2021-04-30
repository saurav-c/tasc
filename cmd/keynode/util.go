package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	cmn "github.com/saurav-c/tasc/lib/common"
	"github.com/saurav-c/tasc/lib/storage"
	kpb "github.com/saurav-c/tasc/proto/keynode"
	tpb "github.com/saurav-c/tasc/proto/tasc"
	log "github.com/sirupsen/logrus"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type IndexType uint

const (
	COMMITTED_INDEX IndexType = iota
	PENDING_INDEX
)

type TransactionSet struct {
	txnSetMap map[string]*tpb.TransactionWriteSet
	mutex     *sync.RWMutex
	manager   storage.StorageManager
}

func NewTransactionSet(manager storage.StorageManager) TransactionSet {
	return TransactionSet{
		txnSetMap: map[string]*tpb.TransactionWriteSet{},
		mutex:     &sync.RWMutex{},
		manager:   manager,
	}
}

func (ts *TransactionSet) get(tid string) (*tpb.TransactionWriteSet, bool) {
	ts.mutex.RLock()
	txnSet, ok := ts.txnSetMap[tid]
	ts.mutex.RUnlock()

	if !ok {
		// Fetch from storage
		log.Debugf("Fetching transaction set %s from storage", tid)
		data, err := ts.manager.Get(tid)
		if err != nil {
			log.Errorf("Error fetching transaction set %s from storage: %s", tid, err.Error())
			os.Exit(1)
		}
		txnSet = &tpb.TransactionWriteSet{}
		proto.Unmarshal(data, txnSet)
		ts.mutex.Lock()
		ts.txnSetMap[tid] = txnSet
		ts.mutex.Unlock()
	}

	return txnSet, true
}

func (ts *TransactionSet) put(tid string, txnWriteSet *tpb.TransactionWriteSet) {
	ts.mutex.Lock()
	ts.txnSetMap[tid] = txnWriteSet
	ts.mutex.Unlock()
}

func (ts *TransactionSet) remove(tid string) {
	ts.mutex.Lock()
	delete(ts.txnSetMap, tid)
	ts.mutex.Unlock()
}

type VersionIndex struct {
	index       map[string]*kpb.KeyVersionList
	locks       map[string]*sync.RWMutex
	mutex       *sync.RWMutex
	indexFormat string
}

func NewVersionIndex(kind IndexType) VersionIndex {
	format := ""
	if kind == COMMITTED_INDEX {
		format = "commit-index:%s"
	} else if kind == PENDING_INDEX {
		format = "pending-index:%s"
	} else {
		log.Debugf("Unknown index type %d", kind)
		os.Exit(1)
	}
	return VersionIndex{
		index:       map[string]*kpb.KeyVersionList{},
		locks:       map[string]*sync.RWMutex{},
		mutex:       &sync.RWMutex{},
		indexFormat: format,
	}
}

/**
Fetch index from storage if it exists
PRE: KeyLock should be LOCKED
POST: Returns TRUE if index found in storage and updates index
*/
func (idx *VersionIndex) readFromStorage(key string, storageManager storage.StorageManager) (*kpb.KeyVersionList, bool) {
	log.Infof("Trying to read index %s from storage", key)
	data, err := storageManager.Get(fmt.Sprintf(idx.indexFormat, key))
	log.Infof("Read index %s from storage", key)
	if err != nil {
		if strings.Contains(err.Error(), "KEY_DNE") {
			return nil, false
		} else {
			log.Errorf("Error reading index %s from storage: %s", key, err.Error())
			os.Exit(1)
		}
	}
	keyVersionList := &kpb.KeyVersionList{}
	proto.Unmarshal(data, keyVersionList)
	idx.mutex.Lock()
	fmt.Printf("Key %s acquired INDEX LOCK\n", key)
	idx.index[key] = keyVersionList
	idx.mutex.Unlock()
	fmt.Printf("Key %s released INDEX LOCK\n", key)
	return keyVersionList, true
}

/**
Write index to storage
PRE: KeyLock should be LOCKED
POST: Index should be written to storage
*/
func (idx *VersionIndex) writeToStorage(key string, storageManager storage.StorageManager) {
	idx.mutex.RLock()
	versionList := idx.index[key]
	idx.mutex.RUnlock()

	data, _ := proto.Marshal(versionList)
	err := storageManager.Put(fmt.Sprintf(idx.indexFormat, key), data)
	if err != nil {
		log.Errorf("Error writing index % to storage: %s", key, err.Error())
		os.Exit(1)
	}
}

/**
Creates key lock and key version list if entries do not exist for KEY.
Pre: No locks required
Post: Returns key lock and key version list
*/
func (idx *VersionIndex) create(key string, storageManager storage.StorageManager) (*sync.RWMutex, *kpb.KeyVersionList) {
	log.Infof("Creating key version state for %s", key)
	idx.mutex.Lock()

	fmt.Printf("Key %s acquired INDEX LOCK\n", key)

	var keyLock *sync.RWMutex
	var versionList *kpb.KeyVersionList
	if kLock, ok := idx.locks[key]; !ok {
		keyLock = &sync.RWMutex{}
		versionList = &kpb.KeyVersionList{}
		keyLock.Lock()
		idx.locks[key] = keyLock
		idx.index[key] = versionList
		idx.mutex.Unlock()

		fmt.Printf("Key %s released INDEX LOCK\n", key)

		// Check storage if index exists
		log.Infof("Fetching from storage")
		fmt.Printf("Key %s going to storage\n", key)
		if storageVersionList, ok := idx.readFromStorage(key, storageManager); ok {
			versionList = storageVersionList
		}
		fmt.Printf("Key %s returned from storage\n", key)
		log.Infof("Returned fetch from storage")
		keyLock.Unlock()
	} else {
		keyLock = kLock
		keyLock.RLock()
		versionList = idx.index[key]
		keyLock.RUnlock()
		idx.mutex.Unlock()

		fmt.Printf("Key %s released INDEX LOCK\n", key)
	}
	return keyLock, versionList
}

func (idx *VersionIndex) updateIndex(tid string, keyVersions []string, toInsert bool,
	storageManager storage.StorageManager, monitor *cmn.StatsMonitor, monitorMsg string) {
	var wg sync.WaitGroup
	wg.Add(len(keyVersions))

	log.Debug("Inside update index")

	for _, keyVersion := range keyVersions {
		go func(kv string) {
			log.Debug("Inside update index for key %s", kv)
			defer wg.Done()
			split := strings.Split(kv, cmn.KeyDelimeter)
			key, version := split[0], split[1]

			idx.mutex.RLock()
			keyLock, ok := idx.locks[key]
			var versionList *kpb.KeyVersionList

			if !ok {
				idx.mutex.RUnlock()
				keyLock, _ = idx.create(key, storageManager)
				idx.mutex.RLock()
			}
			keyLock.Lock()
			versionList = idx.index[key]
			idx.mutex.RUnlock()

			if toInsert {
				insertVersion(versionList, version)
			} else {
				deleteVersion(versionList, version)
			}

			// Write index to storage
			start := time.Now()
			log.Debugf("Writing index %s to storage", key)
			idx.writeToStorage(key, storageManager)
			end := time.Now()
			log.Debugf("Done writing index %s to storage", key)

			keyLock.Unlock()
			go monitor.TrackStat(tid, monitorMsg, end.Sub(start))
		}(keyVersion)
	}
	wg.Wait()
}

func searchVersion(entry *kpb.KeyVersionList, version string) int {
	return sort.Search(len(entry.Versions), func(i int) bool {
		return cmn.CompareKeyVersion(entry.Versions[i], version) >= 0
	})
}

func insertVersion(entry *kpb.KeyVersionList, version string) {
	i := searchVersion(entry, version)
	entry.Versions = append(entry.Versions, "")
	copy(entry.Versions[i+1:], entry.Versions[i:])
	entry.Versions[i] = version
}

func deleteVersion(entry *kpb.KeyVersionList, version string) {
	i := searchVersion(entry, version)
	if i != len(entry.Versions) {
		entry.Versions = append(entry.Versions[:i], entry.Versions[i+1:]...)
	}
}
