package main

const (
	TRANSACTION_SUCCESS = 0;
	TRANSACTION_FAILURE = 1;
)

func (k *KeyNode) readKey (tid string, key string, readList []string, begints string, lowerBound string) (txnid string, keyVersion string, value []byte, coWritten []string) {

	return tid, "", nil, nil
}

func (k *KeyNode) validate (tid string, txnBeginTS string, txnCommitTS string, keys []string, writeSet []string) (txnid string, action int, writeBuffer map[string][]byte) {
	for _, key := range keys {
		pendingKeyVersions := k.pendingKeyVersionIndex[key]
		for _, keyVersion := range pendingKeyVersions {
			keyCommitTS := keyVersion.CommitTS
			if txnBeginTS < keyCommitTS && keyCommitTS < txnCommitTS {
				return tid, 1, nil
			}
		}
	}

	return tid, 0, nil
}

func (k *KeyNode) endTransaction (txnid string, action int, writeBuffer map[string][]byte) {

}