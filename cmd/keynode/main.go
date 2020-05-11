package main

import (
	"flag"
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

func _convertStringToBytes(stringSlice []string) ([]byte) {
	stringByte := strings.Join(stringSlice, "\x20\x00")
	return []byte(stringByte)
}

func _convertBytesToString(byteSlice []byte) ([]string) {
	stringSliceConverted := string(byteSlice)
	return strings.Split(stringSliceConverted, "\x20\x00")
}

func (k *KeyNode) _deleteFromPendingKVI (keys []string, keyEntry string, action int8) {
	for _, key := range keys {
		k.pendingLock.RLock()
		keyLock := k.pendingKeysLock[key]
		k.pendingLock.RUnlock()

		k.pendingKeyVersionIndexLock.RLock()
		pendingEntry := k.pendingKeyVersionIndex[key]
		k.pendingKeyVersionIndexLock.RUnlock()

		keyLock.Lock()
		indexEntry := FindIndex(pendingEntry.keys, keyEntry)
		pendingEntry.keys = append(pendingEntry.keys[:indexEntry], pendingEntry.keys[indexEntry+1:]...)
		keyLock.Unlock()
	}

	if action == TRANSACTION_FAILURE {
		return
	}

	kviKeys := make([]string, len(keys))
	kviValBytes := make([][]byte, len(keys))

	for i, key := range keys {
		// Acquire read lock on map that stores committed locks, and check if it exists
		k.committedLock.RLock()
		cLock, ok := k.committedKeysLock[key]
		k.committedLock.RUnlock()

		var entry *keysList
		if !ok {
			k.committedLock.Lock()
			k.keyVersionIndexLock.Lock()

			cLock = &sync.RWMutex{}
			k.committedKeysLock[key] = cLock
			entry = &keysList{keys: make([]string, 0)}
			k.keyVersionIndex[key] = entry

			k.committedLock.Unlock()
			k.keyVersionIndexLock.Unlock()
		} else {
			k.keyVersionIndexLock.RLock()
			entry = k.keyVersionIndex[key]
			k.keyVersionIndexLock.RUnlock()
		}

		cLock.Lock()
		entry.keys = InsertParticularIndex(entry.keys, keyEntry)
		cLock.Unlock()

		dbKey := key + ":index"
		if k.batchMode {
			k._addToBuffer(dbKey, _convertStringToBytes(entry.keys))
		} else {
			kviKeys[i] = dbKey
			kviValBytes[i] = _convertStringToBytes(entry.keys)
		}
	}

	if !k.batchMode {
		k.StorageManager.MultiPut(kviKeys, kviValBytes)
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

func (k KeyNode) _addToBuffer(key string, value []byte) {
	k.commitLock.Lock()
	k.commitBuffer[key] = value
	k.commitLock.Unlock()
}

func (k KeyNode) _flushBuffer() error {
	k.commitLock.Lock()
	copyCommitBuffer := k.commitBuffer
	k.commitLock.Unlock()
	allKeys := make([]string, 0)
	allValues := make([][]byte, 0)
	for k, v := range copyCommitBuffer {
		allKeys = append(allKeys, k)
		allValues = append(allValues, v)
	}
	keysWritten, err := k.StorageManager.MultiPut(allKeys, allValues)
	k.commitLock.Lock()
	defer k.commitLock.Unlock()
	if err != nil {
		for _, key := range keysWritten {
			delete(k.commitBuffer, key)
		}
		return errors.New("Not all keys have been put")
	}
	for key := range copyCommitBuffer {
		delete(k.commitBuffer, key)
	}
	return nil
}

func (k *KeyNode) readKey (tid string, key string, readList []string, begints string, lowerBound string) (keyVersion string, value []byte, coWritten []string, err error) {
	// Check for Index Lock
	k.committedLock.Lock()
	if _, ok := k.committedKeysLock[key]; !ok {
		// Fetch index from storage
		start := time.Now()
		index, err := k.StorageManager.Get(key + ":" + "index")
		end := time.Now()
		// No Versions found for this key
		if err != nil {
			k.committedLock.Unlock()
			return "", nil, nil, errors.New("Key not found")
		}
		fmt.Printf("Index Read: %f\n", end.Sub(start).Seconds())
		k.committedKeysLock[key] = &sync.RWMutex{}
		k.keyVersionIndexLock.Lock()
		k.keyVersionIndex[key] = &keysList{keys: _convertBytesToString(index)}
		k.keyVersionIndexLock.Unlock()
	}
	k.committedLock.Unlock()

	k.committedLock.RLock()
	keyLock := k.committedKeysLock[key]
	k.keyVersionIndexLock.RUnlock()

	keyLock.RLock()
	keyVersions := k.keyVersionIndex[key].keys
	keyLock.RUnlock()

	// TODO Check if the most recent version is older than the lower bound
	// In this case, we need to block this read because the update has not
	// yet propagated to this Key Node. This will be done via blocking on channels that get
	// updated when the Key Node adds entries to the Key Version Index.

	var version string
	for i := range(keyVersions) {
		version = keyVersions[len(keyVersions) - 1 - i]

		splits := strings.Split(version, KEY_VERSION_DELIMITER)
		keyCommitTS, keyTxn := splits[0], splits[1]

		// Check lower bound
		if lowerBound != "" && lowerBound > keyCommitTS {
			continue
		}

		// Check to make sure this version existed when Txn began
		if keyCommitTS >= begints {
			continue
		}

		// Check compatibility of this txn's readSet with this key's cowrittenset
		var coWrites []string
		k.committedTxnCacheLock.RLock()
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
		k.committedTxnCacheLock.RUnlock()
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
		// Acquire a Read Lock on map that stores pending locks
		k.pendingLock.RLock()

		// Check if the pending lock for this key exists
		pLock, ok := k.pendingKeysLock[key]

		// Release the Read Lock on map that stores pending locks
		k.pendingLock.RUnlock()

		if ok {
			// Acquire read lock for this key's pending version lock
			pLock.RLock()

			// Acquire a read lock to read the pending KVI entry, then release it
			k.pendingKeyVersionIndexLock.RLock()
			pendingEntry := k.pendingKeyVersionIndex[key]
			k.pendingKeyVersionIndexLock.RUnlock()

			for _, keyVersion := range pendingEntry.keys {
				keyCommitTS := strings.Split(keyVersion, KEY_VERSION_DELIMITER)[0]
				if txnBeginTS < keyCommitTS && keyCommitTS < txnCommitTS {
					pLock.RUnlock()
					return TRANSACTION_FAILURE
				}
			}

			// Release read lock for this key's pending version lock
			pLock.RUnlock()
		}

		// Check for write conflicts in committed Key Version Index
		// Acquire a read lock on map that stores committed locks
		k.committedLock.RLock()

		// Check if committed lock for this key exists
		cLock, ok := k.committedKeysLock[key]

		// Release read lock on map that stores committed locks
		k.committedLock.RUnlock()

		if ok {
			// Acquire read lock for this key's committed version lock
			cLock.RLock()

			// Acquire a read lock to read committed KVI entry, then release it
			k.keyVersionIndexLock.RLock()
			entry := k.keyVersionIndex[key]
			k.keyVersionIndexLock.RUnlock()

			for _, keyVersion := range entry.keys {
				keyCommitTS := strings.Split(keyVersion, KEY_VERSION_DELIMITER)[0]
				if txnBeginTS < keyCommitTS && keyCommitTS < txnCommitTS {
					cLock.RUnlock()
					return TRANSACTION_FAILURE
				}
			}
			cLock.RUnlock()
		}
	}

	// No conflicts found in pending or committed KVI

	// Insert keys into pending Key Version Index
	keyVersion := txnCommitTS + KEY_VERSION_DELIMITER + tid

	for _, key := range keys {
		// Get read lock on map that holds pending locks
		k.pendingLock.RLock()
		keyLock, ok := k.pendingKeysLock[key]
		k.pendingLock.RUnlock()

		var pendingEntry *keysList
		// Check if pending lock exists for this key, if not create the entries
		if !ok {
			k.pendingLock.Lock()
			k.pendingKeyVersionIndexLock.Lock()

			keyLock = &sync.RWMutex{}
			k.pendingKeysLock[key] = keyLock

			pendingEntry = &keysList{keys: make([]string, 1)}
			k.pendingKeyVersionIndex[key] = pendingEntry

			k.pendingLock.Unlock()
			k.pendingKeyVersionIndexLock.Unlock()
		} else {
			k.pendingKeyVersionIndexLock.RLock()
			pendingEntry = k.pendingKeyVersionIndex[key]
			k.pendingKeyVersionIndexLock.RUnlock()
		}

		keyLock.Lock()
		pendingEntry.keys = InsertParticularIndex(pendingEntry.keys, keyVersion)
		keyLock.Unlock()
	}

	// Add entry to pending transaction writeset
	k.pendingTxnCacheLock.Lock()
	k.pendingTxnCache[tid] = &pendingTxn{
		keys:     keys,
		keyVersion: keyVersion,
	}
	k.pendingTxnCacheLock.Unlock()
	return TRANSACTION_SUCCESS
}

func (k *KeyNode) endTransaction (tid string, action int8, writeBuffer map[string][]byte) (error) {
	// Acquire read lock on pending Txn Cache, return error if not found
	k.pendingTxnCacheLock.RLock()
	pendingTxn, ok := k.pendingTxnCache[tid]
	k.pendingTxnCacheLock.RUnlock()
	if !ok {
		return errors.New("Transaction not found")
	}

	// TODO: Turn this into gc, add some state to indicate no longer pending
	//k.pendingTxnCacheLock.Lock()
	//delete(k.pendingTxnCache, tid)
	//k.pendingTxnCacheLock.Unlock()

	TxnKeys := pendingTxn.keys
	keyVersion := pendingTxn.keyVersion

	if action == TRANSACTION_FAILURE {
		// Deleting the entries from the Pending Key-Version Index
		k._deleteFromPendingKVI(TxnKeys, keyVersion, TRANSACTION_FAILURE)
		return nil
	}

	// Add to committed Txn Writeset and Read Cache
	s := time.Now()
	var writeSet []string
	for key := range writeBuffer {
		writeSet = append(writeSet, key + KEY_DELIMITER + keyVersion)
	}
	k.committedTxnCacheLock.Lock()
	k.committedTxnCache[tid] = writeSet
	k.committedTxnCacheLock.Unlock()
	e := time.Now()
	fmt.Printf("TxnWrite Time: %f\n\n", 1000 * e.Sub(s).Seconds())


	// TODO: Adding to read cache and deleting from pendingKVI can be done with goroutines, if
	// TODO: we block if value not in read cache yet
	s = time.Now()
	k.readCacheLock.Lock()
	for key, value := range writeBuffer {
		k.readCache[key + KEY_DELIMITER + keyVersion] = value
	}
	k.readCacheLock.Unlock()
	e = time.Now()
	fmt.Printf("Read Cache Time: %f\n\n", 1000 * e.Sub(s).Seconds())

	// Deleting the entries from the Pending Key-Version Index and storing in Committed Txn Cache
	s = time.Now()
	k._deleteFromPendingKVI(TxnKeys, keyVersion, TRANSACTION_SUCCESS)
	e = time.Now()
	fmt.Printf("Delete PKVI Time: %f\n\n", 1000 * e.Sub(s).Seconds())

	return nil
}

func main() {
	storage := flag.String("storage", "dynamo", "Storage Engine")
	batchMode := flag.Bool("batch", false, "Whether to do batch updates or not")
	flag.Parse()
	fmt.Printf("Batch Mode: %t\n", *batchMode)

	keyNode, err := NewKeyNode(*storage, *batchMode)
	if err != nil {
		log.Fatalf("Could not start new Key Node %v\n", err)
	}
	if *batchMode {
		go flusher(keyNode)
	}
	startKeyNode(keyNode)
}