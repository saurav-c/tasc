package main

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

import (
	"errors"
	"math/rand"
)

const (
	TRANSACTION_SUCCESS = 0;
	TRANSACTION_FAILURE = 1;
	KEY_DELIMITER = ":";
	KEY_VERSION_DELIMITER = "-";
)

func (k *KeyNode) _deleteFromPendingKVI (keys []string, keyEntry string, action int8) {
	for _, key := range keys {
		k.pendingKeyVersionIndexLock[key].Lock()
		pendingKeyVersions := k.pendingKeyVersionIndex[key]

		indexEntry := FindIndex(pendingKeyVersions, keyEntry)
		newPendingKeyVersions := append(pendingKeyVersions[:indexEntry], pendingKeyVersions[indexEntry+1:]...)

		k.pendingKeyVersionIndex[key] = newPendingKeyVersions
		k.pendingKeyVersionIndexLock[key].Unlock()
	}

	if action == TRANSACTION_FAILURE {
		return
	}

	for _, key := range keys {
		// Acquire key index lock
		if _, ok := k.keyVersionIndexLock[key]; !ok {
			k.createLock.Lock()
			k.keyVersionIndexLock[key] = &sync.RWMutex{}
			k.createLock.Unlock()
		}

		k.keyVersionIndexLock[key].Lock()

		// Perform key index insert
		committedKeyVersions := k.keyVersionIndex[key]
		committedKeyVersions = InsertParticularIndex(committedKeyVersions, keyEntry)

		// Modify key version index
		k.keyVersionIndex[key] = committedKeyVersions

		k.keyVersionIndexLock[key].Unlock()
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

func (k *KeyNode) readKey (tid string, key string, readList []string, begints string, lowerBound string) (keyVersion string, value []byte, coWritten []string, err error) {
	// Check for Index Lock
	if _, ok := k.keyVersionIndexLock[key]; !ok {
		// Fetch index from storage
		// TODO: Implement writing key version index to storage
		start := time.Now()
		_, _ = k.StorageManager.Get(key + ":" + "index")
		end := time.Now()
		fmt.Printf("Index Read: %f\n", end.Sub(start).Seconds())

		start = time.Now()
		defVal, _ := k.StorageManager.Get(key + KEY_DELIMITER + "0")
		end = time.Now()
		fmt.Printf("Key Read: %f\n", end.Sub(start).Seconds())

		return "default", defVal, nil, nil
	}

	k.keyVersionIndexLock[key].RLock()
	keyVersions := k.keyVersionIndex[key]
	k.keyVersionIndexLock[key].RUnlock()

	// TODO Check if the most recent version is older than the lower bound
	// In this case, we need to block this read because the update has not
	// yet propagated to this Key Node. This will be done via blocking on channels that get
	// updated when the Key Node adds entries to the Key Version Index.

	var version string
	for i := range(keyVersions) {
		version = keyVersions[len(keyVersions) - 1 - i]

		splits := strings.Split(version, KEY_VERSION_DELIMITER)
		keyCommitTS, keyTxn := splits[0], splits[1]

		// Check lowerbound
		if lowerBound != "" && lowerBound > keyCommitTS {
			continue
		}

		// Check to make sure this version existed when Txn began
		if keyCommitTS >= begints {
			continue
		}

		// Check compatibility of this txn's readSet with this key's cowrittenset
		var coWrites []string
		if writeSet, ok := k.committedTxnCache[keyTxn]; ok {
			coWrites = writeSet
			writeVersions := make(map[string]string)
			for _, wVersion := range writeSet {
				wSplit := strings.Split(wVersion, KEY_DELIMITER)
				writeVersions[wSplit[0]] = strings.Split(wSplit[1], KEY_VERSION_DELIMITER)[0]
			}

			invalidVersion := false
			for _, readVersion := range readList {
				rSplit := strings.Split(readVersion, KEY_DELIMITER)
				if wVers, ok := writeVersions[rSplit[0]]; ok {
					rCommitTS := strings.Split(rSplit[1], KEY_VERSION_DELIMITER)[0]
					if wVers > rCommitTS {
						invalidVersion = true
						break
					}
				}
			}
			if invalidVersion {
				continue
			}
		}
		keyToUse := key + KEY_DELIMITER + version
		// Reaching this line means the version is valid
		// Check for version in cache
		k.readCacheLock.RLock()
		if val, ok := k.readCache[keyToUse]; ok {
			k.readCacheLock.RUnlock()
			return version, val, coWrites, nil
		}
		k.readCacheLock.RUnlock()

		// Fetch value from storage manager
		val, err := k.StorageManager.Get(keyToUse)
		if err != nil {
			return version, val, coWrites, errors.New("Error fetching value from storage")
		}

		k.readCacheLock.Lock()
		k.readCache[version] = val
		k.readCacheLock.Unlock()

		return version, val, coWrites, nil
	}
	return"", nil, nil, errors.New("No valid version found!")
}

func (k *KeyNode) validate (tid string, txnBeginTS string, txnCommitTS string, keys []string) (action int8) {
	for _, key := range keys {
		// Check for write conflicts in pending Key Version Index
		if lock, ok := k.pendingKeyVersionIndexLock[key]; ok {
			lock.RLock()
			pendingKeyVersions := k.pendingKeyVersionIndex[key];
			for _, keyVersion := range pendingKeyVersions {
				keyCommitTS := strings.Split(keyVersion, KEY_VERSION_DELIMITER)[0]
				if txnBeginTS < keyCommitTS && keyCommitTS < txnCommitTS {
					lock.RUnlock()
					return TRANSACTION_FAILURE
				}
			}
			lock.RUnlock()
		} else {
			// Need to create a new lock for this pending Key and the pending slice
			k.createPendingLock.Lock()
			k.pendingKeyVersionIndexLock[key] = &sync.RWMutex{}
			k.createPendingLock.Unlock()

			k.pendingKeyVersionIndexLock[key].Lock()
			k.pendingKeyVersionIndex[key] = make([]string, 1)
			k.pendingKeyVersionIndexLock[key].Unlock()
		}

		// Check for write conflicts in committed Key Version Index
		if lock, ok := k.keyVersionIndexLock[key]; ok {
			lock.RLock()
			keyVersions := k.keyVersionIndex[key];
			for _, keyVersion := range keyVersions {
				keyCommitTS := strings.Split(keyVersion, KEY_VERSION_DELIMITER)[0]
				if txnBeginTS < keyCommitTS && keyCommitTS < txnCommitTS {
					lock.RUnlock()
					return TRANSACTION_FAILURE
				}
			}
			lock.RUnlock()
		}
	}

	// Insert keys into pending Key Version Index
	keyVersion := txnCommitTS + KEY_VERSION_DELIMITER + tid
	for _, key := range keys {
		k.pendingKeyVersionIndexLock[key].Lock()
		pendingKeyVersions := k.pendingKeyVersionIndex[key]
		updatedKeyVersions := InsertParticularIndex(pendingKeyVersions, keyVersion)
		k.pendingKeyVersionIndex[key] = updatedKeyVersions
		k.pendingKeyVersionIndexLock[key].Unlock()
	}

	// Add entry to pending transaction writeset
	k.pendingTxnCache[tid] = &pendingTxn{
		keys:     keys,
		keyVersion: keyVersion,
	}
	return TRANSACTION_SUCCESS
}

func (k *KeyNode) endTransaction (tid string, action int8, writeBuffer map[string][]byte) (error) {
	pendingTxn, ok := k.pendingTxnCache[tid]
	if !ok {
		return errors.New("Transaction not found")
	}
	delete(k.pendingTxnCache, tid)
	TxnKeys := pendingTxn.keys
	keyVersion := pendingTxn.keyVersion

	if action == TRANSACTION_FAILURE {
		// Deleting the entries from the Pending Key-Version Index
		k._deleteFromPendingKVI(TxnKeys, keyVersion, TRANSACTION_FAILURE)
		return nil
	}


	// Add to committed Txn Writeset and Read Cache
	var writeSet []string
	for key := range writeBuffer {
		writeSet = append(writeSet, key + KEY_DELIMITER + keyVersion)
	}
	k.committedTxnCache[tid] = writeSet

	// Deleting the entries from the Pending Key-Version Index and storing in Committed Txn Cache
	k._deleteFromPendingKVI(TxnKeys, keyVersion, TRANSACTION_SUCCESS)

	for key, value := range writeBuffer {
		k.readCache[key + KEY_DELIMITER + keyVersion] = value
	}

	return nil
}

func main() {
	ip := ""
	keyNode, err := NewKeyNode(ip, "local")
	if err != nil {
		log.Fatalf("Could not start new Key Node %v\n", err)
	}
	fmt.Println("hello world")

	startKeyNode(keyNode)
	fmt.Println("whats up")
}