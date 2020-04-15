package storage

import (
	pb "github.com/saurav-c/aftsi/proto/aftsi/api"
)

const TransactionKey = "transactions/%s-%d"

type StorageManager interface {
	// Start a new transaction with the execution ID passed in; this ID will be
	// used for all operations relevant to this particular transaction.
	StartTransaction(id string) error

	// Commit all of the changes made in this transaction to the storage engine.
	CommitTransaction(transaction *pb.TransactionRecord) error

	// Retrieve a transaction record from the storage engine.
	GetTransaction(transactionKey string) (*pb.TransactionRecord, error)

	// Retrieve a set of transactions from the storage engine.
	MultiGetTransaction(transactionKeys *[]string) (*[]*pb.TransactionRecord, error)

	// As a part of the transaction owned by tid, insert a key-value pair into
	// the storage engine.
	Put(key string, val *pb.KeyValuePair) error

	// As a part of transaction owned by tid, insert a set of key-value pairs
	// into the storage engine.
	MultiPut(*map[string]*pb.KeyValuePair) error

	// Retrieve the given key as a part of the transaction tid.
	Get(key string) (*pb.KeyValuePair, error)

	// Returns a list of the keys that start with the given prefix.
	List(prefix string) ([]string, error)

	// Deletes the given key from the underlying storage engine.
	Delete(key string) error

	// Delete multiple keys at once.
	MultiDelete(*[]string) error
}
