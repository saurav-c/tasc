package main

import (
	"errors"
	cmn "github.com/saurav-c/tasc/lib/common"
	kpb "github.com/saurav-c/tasc/proto/keynode"
	tpb "github.com/saurav-c/tasc/proto/tasc"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"
)

func (k *KeyNode) readKey(tid string, key string, readSet []string, beginTs int64,
	lowerBound string) (string, []byte, []string, error) {
	var keyVersions []string

	k.CommittedVersionIndex.mutex.RLock()
	keyLock, ok := k.CommittedVersionIndex.locks[key]
	if ok {
		keyLock.RLock()
		keyVersions = k.CommittedVersionIndex.index[key].Versions
		keyLock.RUnlock()
		k.CommittedVersionIndex.mutex.RUnlock()
	} else {
		// Version Index does not exist
		k.CommittedVersionIndex.mutex.RUnlock()

		start := time.Now()
		_, keyVersionList := k.CommittedVersionIndex.create(key, k.StorageManager, tid, "[READ] Storage Read Committed Index", k.Monitor)
		keyVersions = keyVersionList.Versions
		end := time.Now()
		go k.Monitor.TrackStat(tid, "[READ] Create Committed Version State", end.Sub(start))
	}

	lowerBoundVersion := ""
	if lowerBound != "" {
		split := strings.Split(lowerBound, cmn.KeyDelimeter)
		lowerBoundVersion = split[1]
	}

	var timeoutStart time.Time
	first := true
	start := time.Now()
	for {
		for i := len(keyVersions) - 1; i >= 0; i-- {
			version := keyVersions[i]

			// Check lower bound
			if cmn.CompareKeyVersion(lowerBoundVersion, version) > 0 {
				break
			}

			splits := strings.Split(version, cmn.VersionDelimeter)
			vCommitTsStr, vTxnId := splits[0], splits[1]
			vCommitTs := cmn.Int64FromString(vCommitTsStr)

			// Check to make sure this version existed when Txn began
			if vCommitTs >= beginTs {
				continue
			}

			// Check compatibility of this txn's readSet with this versions's cowrittenset
			coWrittenSet, ok := k.isCompatibleVersion(vTxnId, readSet)
			if !ok {
				continue
			}

			end := time.Now()
			go k.Monitor.TrackStat(tid, "[READ] Compute Read Version", end.Sub(start))

			storageKeyVersion := key + cmn.KeyDelimeter + version
			return storageKeyVersion, nil, coWrittenSet, nil
		}

		if first {
			first = false
			timeoutStart = time.Now()
		}

		// No valid versions found
		if time.Now().Sub(timeoutStart) >= 20 * time.Millisecond {
			log.Errorf("Timed out, no valid versions found for %s", key)
			break
		}

		// Sleep and retry
		log.Debugf("Sleeping and retrying to find versions for %s", key)
		time.Sleep(5 * time.Millisecond)
	}
	return "", nil, nil, errors.New("no valid version found")
}

func (k *KeyNode) isCompatibleVersion(versionTid string, readSet []string) ([]string, bool) {
	writeSet, _ := k.CommittedTxnSet.get(versionTid)
	writeSetVersions := map[string]string{}
	for _, keyVersion := range writeSet.Keys {
		split := strings.Split(keyVersion, cmn.KeyDelimeter)
		key, version := split[0], split[1]
		writeSetVersions[key] = version
	}

	for _, readSetKey := range readSet {
		split := strings.Split(readSetKey, cmn.KeyDelimeter)
		key, readVersion := split[0], split[1]
		if writeSetVersion, ok := writeSetVersions[key]; ok &&
			cmn.CompareKeyVersion(writeSetVersion, readVersion) > 0 {
			return nil, false
		}
	}
	return writeSet.Keys, true
}

func (k *KeyNode) validate(tid string, beginTs int64, commitTs int64, keys []string) (action kpb.TransactionAction) {
	conflictChan := make(chan bool, 2)
	updateChan := make(chan string, len(keys))

	version := cmn.Int64ToString(commitTs) + cmn.VersionDelimeter + tid

	start := time.Now()
	go k.checkPendingConflicts(tid, version, keys, beginTs, commitTs, conflictChan, updateChan)
	go k.checkCommittedConflicts(tid, keys, beginTs, commitTs, conflictChan)

	count := 0
	for conflict := range conflictChan {
		count++
		if conflict {
			log.Debugf("Found conflict, aborting transaction %s", tid)

			// Delete Pending versions that were added
			wg := sync.WaitGroup{}
			wg.Add(len(updateChan))
			for elem := range updateChan {
				go func(key string) {
					defer wg.Done()
					k.PendingVersionIndex.mutex.RLock()
					kLock := k.PendingVersionIndex.locks[key]
					kLock.Lock()
					pendingVersions := k.PendingVersionIndex.index[key]
					k.PendingVersionIndex.mutex.RUnlock()
					deleteVersion(pendingVersions, version)
					kLock.Unlock()
				}(elem)
			}
			wg.Wait()
			return kpb.TransactionAction_ABORT
		}
		if count == 2 {
			break
		}
	}
	end := time.Now()
	go k.Monitor.TrackStat(tid, "[COMMIT] Validation conflict check", end.Sub(start))

	var keyVersions []string
	for _, key := range keys {
		keyVersion := key + cmn.KeyDelimeter + version
		keyVersions = append(keyVersions, keyVersion)
	}
	pendingTxnSet := &tpb.TransactionWriteSet{
		Keys: keyVersions,
	}
	k.PendingTxnSet.put(tid, pendingTxnSet)

	// Persist Pending versions to storage
	wg := sync.WaitGroup{}
	wg.Add(len(keys))

	start = time.Now()
	for _, elem := range keys {
		go func(key string) {
			defer wg.Done()

			k.PendingVersionIndex.mutex.RLock()
			kLock := k.PendingVersionIndex.locks[key]
			kLock.Lock()
			pendingVersions := k.PendingVersionIndex.index[key]
			k.PendingVersionIndex.mutex.RUnlock()
			k.PendingVersionIndex.writeToStorage(key, k.StorageManager, pendingVersions)
			kLock.Unlock()
		}(elem)
	}
	wg.Wait()
	end = time.Now()

	go k.Monitor.TrackStat(tid, "[COMMIT] Storage Write Pending Index", end.Sub(start))
	return kpb.TransactionAction_COMMIT
}

func (k *KeyNode) checkPendingConflicts(tid string, keyVersion string, keys[] string, beginTS int64, commitTS int64,
	conflictChan chan bool, updatedChan chan string) {

	defer k.Monitor.TrackFuncExecTime(tid, "[COMMIT] Pending Conflict Check", time.Now())

	defer close(updatedChan)
	terminate := false // Used to indicate early termination for aborts
	var wg sync.WaitGroup
	wg.Add(len(keys))

	idx := k.PendingVersionIndex
	for _, checkKey := range keys {
		go func(key string) {
			defer wg.Done()
			if terminate {
				return
			}

			idx.mutex.RLock()
			kLock, ok := idx.locks[key]

			// Initialize index entry
			if !ok {
				idx.mutex.RUnlock()
				kLock, _ = idx.create(key, k.StorageManager, tid, "[COMMIT] Storage Read Pending Index", k.Monitor)
				idx.mutex.RLock()
			}

			kLock.Lock()
			defer kLock.Unlock()
			pendingVersionList := idx.index[key]
			pendingVersions := pendingVersionList.Versions
			idx.mutex.RUnlock()

			for i := len(pendingVersions) - 1; i >= 0; i-- {
				if terminate {
					return
				}
				version := pendingVersions[i]
				split := strings.Split(version, cmn.VersionDelimeter)
				versionCommitTS := cmn.Int64FromString(split[0])

				if versionCommitTS <= beginTS {
					break
				}
				if beginTS < versionCommitTS && versionCommitTS < commitTS {
					conflictChan <- true
					terminate = true
					return
				}

				// Insert to Pending KVI
				insertVersion(pendingVersionList, keyVersion)
				updatedChan <- key
			}

		}(checkKey)
	}

	wg.Wait()
	if !terminate {
		conflictChan <- false
	}
}

func (k *KeyNode) checkCommittedConflicts(tid string, keys[] string, beginTS int64, commitTS int64,
	conflictChan chan bool) {

	defer k.Monitor.TrackFuncExecTime(tid, "[COMMIT] Commit Conflict Check", time.Now())
	terminate := false // Used to indicate early termination for aborts
	var wg sync.WaitGroup
	wg.Add(len(keys))

	idx := k.CommittedVersionIndex
	for _, checkKey := range keys {
		go func(key string) {
			defer wg.Done()
			if terminate {
				return
			}

			idx.mutex.RLock()
			kLock, ok := idx.locks[key]

			// Initialize index entry
			if !ok {
				idx.mutex.RUnlock()
				kLock, _ = idx.create(key, k.StorageManager, tid, "[COMMIT] Storage Read Committed Index", k.Monitor)
				idx.mutex.RLock()
			}

			kLock.RLock()
			committedVersions := idx.index[key].Versions
			kLock.RUnlock()
			idx.mutex.RUnlock()

			for i := len(committedVersions) - 1; i >= 0; i-- {
				if terminate {
					return
				}
				version := committedVersions[i]
				split := strings.Split(version, cmn.VersionDelimeter)
				versionCommitTS := cmn.Int64FromString(split[0])

				if versionCommitTS <= beginTS {
					break
				}
				if beginTS < versionCommitTS && versionCommitTS < commitTS {
					conflictChan <- true
					terminate = true
					return
				}
			}

		}(checkKey)
	}

	wg.Wait()
	if !terminate {
		conflictChan <- false
	}
}

func (k *KeyNode) endTransaction(tid string, action kpb.TransactionAction, writeSet []string) error {
	pendingWrites, _ := k.PendingTxnSet.get(tid)

	defer k.localGarbageCollect(tid, pendingWrites)

	if action == kpb.TransactionAction_ABORT {
		return nil
	}

	// Commit txn set and key versions
	txnWriteSet := &tpb.TransactionWriteSet{Keys:writeSet}
	k.CommittedTxnSet.put(tid, txnWriteSet)

	start := time.Now()
	k.CommittedVersionIndex.updateIndex(tid, pendingWrites.Keys, true, k.StorageManager, k.Monitor,
		"[END] Storage Write Committed Index")
	k.PendingVersionIndex.updateIndex(tid, pendingWrites.Keys, false, k.StorageManager, k.Monitor,
		"[END] Storage Write Pending Index")
	end := time.Now()
	go k.Monitor.TrackStat(tid, "[END] Update Committed and Pending Indexes", end.Sub(start))

	return nil
}

func (k *KeyNode) localGarbageCollect(tid string, pendingWrites *tpb.TransactionWriteSet) {
	go k.PendingTxnSet.remove(tid)
}

func (k *KeyNode) shutdown() {
	k.LogFile.Sync()
	k.LogFile.Close()
}

func main() {
	keyNode, err := NewKeyNode()
	if err != nil {
		log.Fatalf("Could not start new Key Node %v\n", err)
	}

	log.Info("Started Key Node")

	go keyNode.Monitor.SendStats(5 * time.Second)

	defer keyNode.shutdown()

	keyNode.listener()
}
