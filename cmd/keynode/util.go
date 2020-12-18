package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	cmn "github.com/saurav-c/tasc/lib/common"
	"github.com/saurav-c/tasc/lib/storage"
	kpb "github.com/saurav-c/tasc/proto/keynode"
	tpb "github.com/saurav-c/tasc/proto/tasc"
	log "github.com/sirupsen/logrus"
	"sort"
	"strings"
	"sync"
)

type TransactionSet struct {
	txnSetMap map[string]*tpb.TransactionWriteSet
	mutex *sync.RWMutex
}

func NewTransactionSet() TransactionSet {
	return TransactionSet{
		txnSetMap: map[string]*tpb.TransactionWriteSet{},
		mutex:     &sync.RWMutex{},
	}
}

func (ts *TransactionSet) get(tid string) (*tpb.TransactionWriteSet, bool) {
	ts.mutex.RLock()
	txnSet, ok := ts.txnSetMap[tid]
	ts.mutex.RUnlock()
	if !ok {
		return nil, false
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
	index map[string]*kpb.KeyVersionList
	locks map[string]*sync.RWMutex
	mutex *sync.RWMutex
}

func NewVersionIndex() VersionIndex {
	return VersionIndex{
		index: map[string]*kpb.KeyVersionList{},
		locks: map[string]*sync.RWMutex{},
		mutex: &sync.RWMutex{},
	}
}

func (idx *VersionIndex) deleteVersions(txnWriteSet *tpb.TransactionWriteSet) {
	for _, keyVersion := range txnWriteSet.Keys {
		split := strings.Split(keyVersion, cmn.KeyDelimeter)
		key, version := split[0], split[1]
		idx.mutex.RLock()
		keyLock := idx.locks[key]
		keyLock.Lock()
		versionList := idx.index[key]
		idx.mutex.RUnlock()
		deleteVersion(versionList, version)
		keyLock.Unlock()
	}
}

func (idx *VersionIndex) commitVersions(txnWriteSet *tpb.TransactionWriteSet, storageManager storage.StorageManager) {
	var wg sync.WaitGroup
	wg.Add(len(txnWriteSet.Keys))

	for _, keyVersion := range txnWriteSet.Keys {
		go func() {
			defer wg.Done()
			split := strings.Split(keyVersion, cmn.KeyDelimeter)
			key, version := split[0], split[1]

			idx.mutex.RLock()
			keyLock, ok := idx.locks[key]
			var versionList *kpb.KeyVersionList

			if !ok {
				idx.mutex.RUnlock()
				idx.mutex.Lock()
				if keyLock, ok = idx.locks[key]; !ok {
					keyLock = &sync.RWMutex{}
					keyLock.Lock()
					idx.locks[key] = keyLock
					versionList = &kpb.KeyVersionList{}
					idx.index[key] = versionList
				} else {
					keyLock.Lock()
					versionList = idx.index[key]
				}
				idx.mutex.Unlock()
			} else {
				keyLock.Lock()
				versionList = idx.index[key]
				idx.mutex.RUnlock()
			}

			insertVersion(versionList, version)
			data, _ := proto.Marshal(versionList)
			keyLock.Unlock()

			// Write index to storage
			storageManager.Put(fmt.Sprintf(cmn.StorageIndexTemplate, key), data)
			log.Debug("wrote key version index to storage")
		}()
	}
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
