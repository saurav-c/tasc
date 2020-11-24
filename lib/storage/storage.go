package storage

import "strings"

const (
	keyVersionDelim = ":"
)

type StorageManager interface {
	// Commit all of the changes made in this transaction to the storage engine.
	//CommitTransaction(tid string, CommitTS string, writeBuffer map[string][]byte) error

	// Retrieve the given key as a part of the transaction tid.
	Get(key string) ([]byte, error)

	// As a part of the transaction owned by tid, insert a key-value pair into
	// the storage engine.
	Put(key string, val []byte) error

	// As a part of the transaction owned by tid, insert multiple key-value pairs into
	// the storage engine.
	MultiPut(keys []string, vals [][]byte) ([]string, error)

	// Retrieve a transaction record from the storage engine.
	GetTransactionWriteSet(transactionKey string) ([]string, error)

	//// Retrieve a set of transactions from the storage engine.
	MultiGetTransactionWriteSet(transactionKeys *[]string) (*[][]string, error)

	//// Deletes the given key from the underlying storage engine.
	Delete(key string) error

	//// Delete multiple keys at once.
	MultiDelete(*[]string) error
}

func _convertBytesToString(byteSlice []byte) ([]string) {
	stringSliceConverted := string(byteSlice)
	return strings.Split(stringSliceConverted, "\x20\x00")
}

func _convertStringToBytes(stringSlice []string) ([]byte) {
	stringByte := strings.Join(stringSlice, "\x20\x00")
	return []byte(stringByte)
}
