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
	// TODO: Check if key does not exist

	k.CommittedVersionIndex.mutex.RLock()
	keyLock := k.CommittedVersionIndex.locks[key]
	keyLock.RLock()
	keyVersions := k.CommittedVersionIndex.index[key]
	keyLock.RUnlock()
	k.CommittedVersionIndex.mutex.RUnlock()

	// TODO: Wait for new versions if lowerBound not satisfied
	if lowerBound != "" {
		if len(keyVersions.Versions) == 0 {
			log.Errorf("Lower bound error for key %s", key)
			return "", nil, nil, errors.New("no versions newer than lower bound")
		}
		split := strings.Split(lowerBound, cmn.KeyDelimeter)
		lowerBoundVersion := split[1]
		mostRecentVersion := keyVersions.Versions[len(keyVersions.Versions) - 1]
		if cmn.CompareKeyVersion(lowerBoundVersion, mostRecentVersion) >= 0 {
			log.Errorf("Lower bound error for key %s", key)
			return "", nil, nil, errors.New("no versions newer than lower bound")
		}
	}

	for i := len(keyVersions.Versions) - 1; i >= 0; i-- {
		version := keyVersions.Versions[i]

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

		storageKeyVersion := key + cmn.KeyDelimeter + version
		val, err := k.StorageManager.Get(storageKeyVersion)
		if err != nil {
			return "", nil, nil, errors.New("error fetching value from storage")
		}
		return storageKeyVersion, val, coWrittenSet, nil
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

	go k.checkPendingConflicts(beginTs, commitTs, keys, conflictChan)
	go k.checkCommittedConflicts(beginTs, commitTs, keys, conflictChan)

	count := 0
	for conflict := range conflictChan {
		count++
		if conflict {
			log.Debugf("Found conflict, aborting transaction %s", tid)
			return kpb.TransactionAction_ABORT
		}
		if count == 2 {
			go log.Debugf("No conflicts found for transaction %s", tid)
			break
		}
	}

	version := cmn.Int64ToString(commitTs) + cmn.VersionDelimeter + tid

	// Add version to pending Key Version Index for each key
	k.addPendingKeyVersion(version, keys)

	var keyVersions []string
	for _, key := range keys {
		keyVersion := key + cmn.KeyDelimeter + version
		keyVersions = append(keyVersions, keyVersion)
	}
	pendingTxnSet := &tpb.TransactionWriteSet{
		Keys: keyVersions,
	}
	k.PendingTxnSet.put(tid, pendingTxnSet)
	return kpb.TransactionAction_COMMIT
}

func (k *KeyNode) checkPendingConflicts(beginTs int64, commitTs int64, keys []string, reportChan chan bool) {
	for _, key := range keys {
		k.PendingVersionIndex.mutex.RLock()
		pLock, ok := k.PendingVersionIndex.locks[key]
		if !ok {
			k.PendingVersionIndex.mutex.RUnlock()
			continue
		}

		pLock.RLock()
		pendingVersions := k.PendingVersionIndex.index[key]
		k.PendingVersionIndex.mutex.RUnlock()

		for _, versions := range pendingVersions.Versions {
			split := strings.Split(versions, cmn.VersionDelimeter)
			versionCommitTsStr := split[0]
			versionCommitTs := cmn.Int64FromString(versionCommitTsStr)
			if beginTs < versionCommitTs && versionCommitTs < commitTs {
				pLock.RUnlock()
				reportChan <- true
			}
		}
		pLock.RUnlock()
	}
	reportChan <- false
}

func (k *KeyNode) checkCommittedConflicts(beginTs int64, commitTs int64, keys []string, reportChan chan bool) {
	for _, key := range keys {
		k.CommittedVersionIndex.mutex.RLock()
		cLock, ok := k.CommittedVersionIndex.locks[key]
		if !ok {
			k.CommittedVersionIndex.mutex.RUnlock()
			continue
		}

		cLock.RLock()
		committedVersions := k.CommittedVersionIndex.index[key]
		k.CommittedVersionIndex.mutex.RUnlock()

		for _, versions := range committedVersions.Versions {
			split := strings.Split(versions, cmn.VersionDelimeter)
			versionCommitTsStr := split[0]
			versionCommitTs := cmn.Int64FromString(versionCommitTsStr)
			if beginTs < versionCommitTs && versionCommitTs < commitTs {
				cLock.RUnlock()
				reportChan <- true
			}
		}
		cLock.RUnlock()
	}
	reportChan <- false
}

func (k *KeyNode) addPendingKeyVersion(version string, keys []string) {
	for _, key := range keys {
		k.PendingVersionIndex.mutex.RLock()
		pLock, ok := k.PendingVersionIndex.locks[key]
		var pendingEntry *kpb.KeyVersionList

		if !ok {
			// Drop read lock and acquire write lock
			k.PendingVersionIndex.mutex.RUnlock()
			k.PendingVersionIndex.mutex.Lock()

			// Need to check that condition is still valid
			if pLock, ok = k.PendingVersionIndex.locks[key]; !ok {
				pLock = &sync.RWMutex{}
				pLock.Lock()
				k.PendingVersionIndex.locks[key] = pLock
				pendingEntry = &kpb.KeyVersionList{}
				k.PendingVersionIndex.index[key] = pendingEntry
			} else {
				pLock.Lock()
				pendingEntry = k.PendingVersionIndex.index[key]
			}
			k.PendingVersionIndex.mutex.Unlock()
		} else {
			pLock.Lock()
			pendingEntry = k.PendingVersionIndex.index[key]
			k.PendingVersionIndex.mutex.RUnlock()
		}

		insertVersion(pendingEntry, version)
		pLock.Unlock()
	}
}

func (k *KeyNode) endTransaction(tid string, action kpb.TransactionAction, writeSet []string) error {
	pendingWrites, ok := k.PendingTxnSet.get(tid)

	if ok {
		defer k.localGarbageCollect(tid, pendingWrites)
	}

	if action == kpb.TransactionAction_ABORT {
		return nil
	}

	// Commit txn set and key versions
	txnWriteSet := &tpb.TransactionWriteSet{Keys:writeSet}
	k.CommittedTxnSet.put(tid, txnWriteSet)

	if ok {
		start := time.Now()
		k.CommittedVersionIndex.commitVersions(pendingWrites, k.StorageManager, k.Monitor)
		end := time.Now()
		go k.Monitor.TrackStat(tid, "Overall commit version index time", end.Sub(start))
	}
	return nil
}

func (k *KeyNode) localGarbageCollect(tid string, pendingWrites *tpb.TransactionWriteSet) {
	go k.PendingTxnSet.remove(tid)
	go k.PendingVersionIndex.deleteVersions(pendingWrites)
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

	go keyNode.Monitor.SendStats(1 * time.Second)

	defer keyNode.shutdown()

	keyNode.listener()
}
