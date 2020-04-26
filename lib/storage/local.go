package storage

import "fmt"

type LocalStoreManager struct {
	database map[string][]byte
}

func NewLocalStoreManager() (*LocalStoreManager) {
	return &LocalStoreManager{database: make(map[string][]byte)}
}

func (local *LocalStoreManager) CommitTransaction(tid string, CommitTS string, writeBuffer map[string][]byte) error {
	writeSet := make([]string, 0)
	for key, value := range writeBuffer {
		newKey := fmt.Sprintf("%s%s%s-%s", key, keyVersionDelim, CommitTS, tid)
		local.Put(newKey, value)
		writeSet = append(writeSet, newKey)
	}
	writeByte := _convertStringToBytes(writeSet)
	local.Put(tid, writeByte)
	return nil
}

func (local *LocalStoreManager) Get(key string) ([]byte, error) {
	return local.database[key], nil
}

func (local *LocalStoreManager) Put(key string, val []byte) error {
	local.database[key] = val
	return nil
}

func (local *LocalStoreManager) MultiPut(keys []string, vals [][]byte) ([]string, error) {
	writtenKeys := make([]string, 0)
	for index, key := range keys {
		valsPerKey := vals[index]
		local.Put(key, valsPerKey)
		writtenKeys = append(writtenKeys, key)
	}
	return writtenKeys, nil
}

func (local *LocalStoreManager) GetTransactionWriteSet(transactionKey string) ([]string, error) {
	writeSetBytes, err := local.Get(transactionKey)
	if err != nil {
		return nil, nil
	}
	writeSet := _convertBytesToString(writeSetBytes)
	return writeSet, nil
}

func (local *LocalStoreManager) MultiGetTransactionWriteSet(transactionKeys *[]string) (*[][]string, error) {
	completeWriteSet := make([][]string, 0)
	for _, elem := range *transactionKeys {
		writeSet, err := local.GetTransactionWriteSet(elem)
		if err != nil {
			return nil, nil
		}
		completeWriteSet = append(completeWriteSet, writeSet)
	}
	return &completeWriteSet, nil
}

func (local *LocalStoreManager) Delete(key string) error {
	delete(local.database, key)
	return nil
}

func (local *LocalStoreManager) MultiDelete(keys *[]string) error {
	for _, key := range *keys {
		delete(local.database, key)
	}
	return nil
}
