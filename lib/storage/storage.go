package storage

const TransactionKey = "transactions/%s-%d"

type StorageManager interface {
	// Start a new transaction with the execution ID passed in; this ID will be
	// used for all operations relevant to this particular transaction.
	// StartTransaction(id string) error

	// Commit all of the changes made in this transaction to the storage engine.
	// CommitTransaction(id string, CommitTS string, writeBuffer map[string][]byte) error

	// Retrieve the given key as a part of the transaction tid.
	Get(key string) ([]byte, error)

	// As a part of the transaction owned by tid, insert a key-value pair into
	// the storage engine.
	Put(key string, val []byte) error

	// TODO: Implement TransactionWriteSet and TransactionCache Methods
	// Retrieve a transaction record from the storage engine.
	//GetTransactionWriteSet(transactionKey string) (*pb.TransactionRecord, error)
	//
	//// Retrieve a set of transactions from the storage engine.
	//MultiGetTransactionWriteSet(transactionKeys *[]string) (*[]*pb.TransactionRecord, error)

	//// Deletes the given key from the underlying storage engine.
	//Delete(key string) error
	//
	//// Delete multiple keys at once.
	//MultiDelete(*[]string) error
}
