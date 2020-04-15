package main

import (
	"errors"
	"math/rand"
)

const (
	TRANSACTION_SUCCESS = 0;
	TRANSACTION_FAILURE = 1;
	KEY_DELIMITER = ":";
)

func (k *KeyNode) _deleteFromPendingKVI (keys []string, keyEntry *keyVersion, action int8) {
	for _, key := range keys {
		pLock := k.pendingKeyVersionIndexLock[key]
		pLock.RLock()
		pendingKeyVersions := k.pendingKeyVersionIndex[key]
		pLock.RUnlock()

		indexEntry := FindIndex(pendingKeyVersions, keyEntry)
		newPendingKeyVersions := append(pendingKeyVersions[:indexEntry], pendingKeyVersions[indexEntry+1:]...)
		pLock.Lock()
		k.pendingKeyVersionIndex[key] = newPendingKeyVersions
		pLock.Unlock()

		// If txn is to be committed, then added to Committed Txn Cache
		if action == TRANSACTION_SUCCESS {
			lock := k.keyVersionIndexLock[key]
			lock.RLock()
			committedKeyVersions := k.keyVersionIndex[key]
			lock.RUnlock()
			// Finding KeyVersion inserted into Pending and adding to Committed
			keyVersion := pendingKeyVersions[indexEntry]
			newCommittedKeyVersions := InsertParticularIndex(committedKeyVersions, keyVersion)

			lock.Lock()
			k.keyVersionIndex[key] = newCommittedKeyVersions
			lock.Unlock()
		}
	}
}

// TODO: Modify Eviction Policy for Read Cache
// Currently evicting from Read Cache randomly
func (k KeyNode) _evictReadCache(n int) {
	k.readCacheLock.Lock()
	defer k.readCacheLock.Unlock()
	for i := 0; i < n; i++ {
		if len(k.readCache) == ReadCacheLimit {
			randInt := rand.Intn(len(k.readCache))
			key := ""
			for key = range k.readCache {
				if randInt == 0 {
					break
				}
				randInt -= 1
			}
			delete(k.readCache, key)
		}
	}

}

func (k *KeyNode) readKey (tid string, key string, readList []string, begints string, lowerBound string) (txnid string, keyVersion string, value []byte, coWritten []string, err error) {
	lock := k.keyVersionIndexLock[key]
	lock.RLock()
	keyVersions := k.keyVersionIndex[key]
	lock.RUnlock()
	for _, keyVersion := range(keyVersions) {
		keyCommitTS := keyVersion.CommitTS
		if lowerBound < keyCommitTS && keyCommitTS < begints {
			keyVersionName := key + KEY_DELIMITER + keyCommitTS
			txnWriteId := keyVersion.tid
			txnWriteSet := k.committedTxnCache[txnWriteId]
			ok := intersection(txnWriteSet, readList)
			if !ok {
				indexElem := findIndex(txnWriteSet, keyVersionName)
				txnCoWritten := append(txnWriteSet[:indexElem], txnWriteSet[indexElem+1:]...)
				// TODO: Fetch Value from Read Cache or Storage (depending on situation)
				k.readCacheLock.RLock()
				if val, ok := k.readCache[keyVersionName]; ok {
					k.readCacheLock.RUnlock()
					return tid, keyVersionName, val, txnWriteSet, nil
				}
				k.readCacheLock.RUnlock()
				val, ok := k.StorageManager.Get(keyVersionName)
				if ok != nil {
					return tid, keyVersionName, nil, txnCoWritten, errors.New("Can't fetch value from storage")
				}
				return tid, keyVersionName, nil, txnCoWritten, nil
			}
		}
	}
	return tid, "", nil, nil, errors.New("Couldn't locate key!")
}

func (k *KeyNode) validate (tid string, txnBeginTS string, txnCommitTS string, keys []string) (txnid string, action int, writeBuffer map[string][]byte) {
	for _, key := range keys {
		pendingKeyVersions := k.pendingKeyVersionIndex[key]
		for _, keyVersion := range pendingKeyVersions {
			keyCommitTS := keyVersion.CommitTS
			if txnBeginTS < keyCommitTS && keyCommitTS < txnCommitTS {
				return tid, TRANSACTION_FAILURE, nil
			}
		}
	}
	NewKeyVersion := keyVersion{tid: tid, CommitTS: txnCommitTS}
	for _, key := range keys {
		lock := k.pendingKeyVersionIndexLock[key]
		lock.RLock()
		pendingKeyVersions := k.pendingKeyVersionIndex[key]
		lock.RUnlock()
		updatedKeyVersions := InsertParticularIndex(pendingKeyVersions, &NewKeyVersion)
		lock.Lock()
		k.pendingKeyVersionIndex[key] = updatedKeyVersions
		lock.Unlock()
	}
	k.pendingTxnCache[tid] = &pendingTxn{
		keys:     keys,
		commitTS: txnCommitTS,
	}
	return tid, TRANSACTION_SUCCESS, nil
}

func (k *KeyNode) endTransaction (tid string, action int, writeBuffer map[string][]byte) {
	pendingTxn, ok := k.pendingTxnCache[tid]
	if !ok {
		return
	}
	delete(k.pendingTxnCache, tid)
	TxnKeys := pendingTxn.keys
	CommitTS := pendingTxn.commitTS
	keyEntry := &keyVersion{
		tid:      tid,
		CommitTS: CommitTS,
	}
	if action == TRANSACTION_FAILURE {
		// Deleting the entries from the Pending Key-Version Index
		k._deleteFromPendingKVI(TxnKeys, keyEntry, TRANSACTION_FAILURE)
		return
	}
	var writeSet []string
	for key := range writeBuffer {
		writeSet = append(writeSet, key)
	}
	// Deleting the entries from the Pending Key-Version Index and storing in Committed Txn Cache
	k._deleteFromPendingKVI(TxnKeys, keyEntry, TRANSACTION_SUCCESS)
	k.committedTxnCache[tid] = writeSet
}