package main

const (
	TRANSACTION_SUCCESS = 0;
	TRANSACTION_FAILURE = 1;
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

		if action == TRANSACTION_SUCCESS {
			lock := k.keyVersionIndexLock[key]
			lock.RLock()
			committedKeyVersions := k.keyVersionIndex[key]
			lock.RUnlock()

			keyVersion := pendingKeyVersions[indexEntry]
			newCommittedKeyVersions := InsertParticularIndex(committedKeyVersions, keyVersion)

			lock.Lock()
			k.keyVersionIndex[key] = newCommittedKeyVersions
			lock.Unlock()
		}
	}
}

func (k *KeyNode) readKey (tid string, key string, readList []string, begints string, lowerBound string) (txnid string, keyVersion string, value []byte, coWritten []string) {

	return tid, "", nil, nil
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
	committedTxnLock := k.commitedTxnCacheLock[tid]
	committedTxnLock.Lock()
	defer committedTxnLock.Unlock()
	k.committedTxnCache[tid] = writeSet
}