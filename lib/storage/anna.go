package storage

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"sync"

	"github.com/golang/protobuf/proto"

	pb "github.com/saurav-c/tasc/proto/aft"
)

const (
	TransactionsKey = "Transactions"
	TransactionKey  = "%s_%d"
)

type AnnaStorageManager struct {
	freeClients []*AnnaClient
	cond        *sync.Cond
}

func (anna *AnnaStorageManager) GetTransactionWriteSet(transactionKey string) ([]string, error) {
	panic("implement me")
}

func (anna *AnnaStorageManager) MultiGetTransactionWriteSet(transactionKeys *[]string) (*[][]string, error) {
	panic("implement me")
}

func NewAnnaStorageManager(ipAddress string, elbAddress string) *AnnaStorageManager {
	clients := []*AnnaClient{}
	for i := 0; i < 20; i++ {
		anna := NewAnnaClient(elbAddress, ipAddress, false, i)
		clients = append(clients, anna)
	}

	return &AnnaStorageManager{
		freeClients: clients,
		cond:        sync.NewCond(&sync.Mutex{}),
	}
}

func (anna *AnnaStorageManager) Get(key string) ([]byte, error) {
	client := anna.getClient()
	defer anna.releaseClient(client)
	bts, err := client.Get(key)
	if err != nil {
		return nil, err
	}
	return bts, err
}

func (anna *AnnaStorageManager) Put(key string, val []byte) error {
	client := anna.getClient()
	defer anna.releaseClient(client)
	_, err := client.Put(key, val)
	return err
}

func (anna *AnnaStorageManager) MultiPut(keys []string, vals [][]byte) ([]string, error) {
	for i, key := range keys {
		err := anna.Put(key, vals[i])
		if err != nil {
			fmt.Printf("Writing %s. ERROR: %v\n", key, err)
			return nil, err
		}
	}
	return nil, nil
}

func (anna *AnnaStorageManager) getClient() *AnnaClient {
	log.Debug("Requested client")
	anna.cond.L.Lock()
	for len(anna.freeClients) == 0 {
		anna.cond.Wait()
	}
	client := anna.freeClients[0]
	anna.freeClients = anna.freeClients[1:]
	anna.cond.L.Unlock()
	log.Debug("Received client")
	return client
}

func (anna *AnnaStorageManager) releaseClient(client *AnnaClient) {
	anna.cond.L.Lock()
	anna.freeClients = append(anna.freeClients, client)
	anna.cond.Signal()
	anna.cond.L.Unlock()
}

func (anna *AnnaStorageManager) StartTransaction(id string) error {
	return nil
}

func (anna *AnnaStorageManager) CommitTransaction(transaction *pb.TransactionRecord) error {
	key := fmt.Sprintf(TransactionKey, transaction.Id, transaction.Timestamp)
	serialized, err := proto.Marshal(transaction)
	if err != nil {
		return err
	}

	client := anna.getClient()
	defer anna.releaseClient(client)
	_, err = client.Put(key, serialized)

	if err != nil {
		return err
	}

	// Add this transaction key to the set of committed transactions.
	// TODO: This set becomes big really fast. Need to figure out how to
	// optimize it.
	txns, _ := client.GetSet(TransactionsKey)

	txns = append(txns, key)
	_, err = client.PutSet(TransactionsKey, txns)

	return err
}

func (anna *AnnaStorageManager) AbortTransaction(transaction *pb.TransactionRecord) error {
	return nil
}

func (anna *AnnaStorageManager) GetTransaction(transactionKey string) (*pb.TransactionRecord, error) {
	result := &pb.TransactionRecord{}

	client := anna.getClient()
	defer anna.releaseClient(client)
	bts, err := client.Get(transactionKey)
	if err != nil {
		return result, err
	}

	err = proto.Unmarshal(bts, result)
	return result, err
}

func (anna *AnnaStorageManager) MultiGetTransaction(transactionKeys *[]string) (*[]*pb.TransactionRecord, error) {
	results := make([]*pb.TransactionRecord, len(*transactionKeys))

	for index, key := range *transactionKeys {
		txn, err := anna.GetTransaction(key)
		if err != nil {
			return &[]*pb.TransactionRecord{}, err
		}

		results[index] = txn
	}

	return &results, nil
}

func (anna *AnnaStorageManager) Delete(key string) error {
	return nil // Anna does not support deletes.
}

func (anna *AnnaStorageManager) MultiDelete(keys *[]string) error {
	return nil // Anna does not support deletes.
}

func (anna *AnnaStorageManager) List(prefix string) ([]string, error) {
	if prefix != "transactions" {
		return nil, errors.New(fmt.Sprintf("Unexpected prefix: %s", prefix))
	}

	client := anna.getClient()
	defer anna.releaseClient(client)
	return client.GetSet(TransactionsKey)
}
