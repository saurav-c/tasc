package storage

import (
	"fmt"
	"os"

	rdslib "github.com/go-redis/redis"
)

type RedisStorageManager struct {
	client *rdslib.ClusterClient
}

func NewRedisStorageManager(address string, password string) *RedisStorageManager {
	rc := rdslib.NewClusterClient(&rdslib.ClusterOptions{
		Addrs: []string{address},
	})
	_, err := rc.Ping().Result()
	if err != nil {
		fmt.Printf("Unexpected error while connecting to Redis client:\n%v\n", err)
		os.Exit(1)
	}
	return &RedisStorageManager{client: rc}
}

func (redis *RedisStorageManager) CommitTransaction(tid string, CommitTS string, writeBuffer map[string][]byte) error {
	writeSet := make([]string, 0)
	for key, val := range writeBuffer {
		newKey := fmt.Sprintf("%s%s%s-%s", key, keyVersionDelim, CommitTS, tid)
		err := redis.client.Set(newKey, val, 0).Err()
		if err != nil {
			return err
		}
		writeSet = append(writeSet, newKey)
	}
	writeSetBytes := _convertStringToBytes(writeSet)
	return redis.client.Set(tid, writeSetBytes, 0).Err()
}

func (redis *RedisStorageManager) Get(key string) ([]byte, error) {
	val, err := redis.client.Get(key).Result()
	if err != nil {
		return nil, err
	}

	return []byte(val), err
}

func (redis *RedisStorageManager) GetTransaction(transactionKey string) ([]string, error) {
	val, err := redis.client.Get(transactionKey).Result()
	if err != nil {
		return nil, err
	}
	result := _convertBytesToString([]byte(val))
	return result, err
}

func (redis *RedisStorageManager) MultiGetTransaction(transactionKeys *[]string) ([][]string, error) {
	results := make([][]string, len(*transactionKeys))
	for index, key := range *transactionKeys {
		txn, err := redis.GetTransaction(key)
		if err != nil {
			return nil, err
		}
		results[index] = txn
	}
	return results, nil
}

func (redis *RedisStorageManager) Put(key string, val []byte) error {
	return redis.client.Set(key, val, 0).Err()
}

func (redis *RedisStorageManager) MultiPut(keys []string, vals [][]byte) error {
	for index, key := range keys {
		valPerKeys := vals[index]
		err := redis.Put(key, valPerKeys)
		if err != nil {
			return err
		}
	}
	return nil
}

func (redis *RedisStorageManager) Delete(key string) error {
	return redis.client.Del(key).Err()
}

func (redis *RedisStorageManager) MultiDelete(keys *[]string) error {
	for _, key := range *keys {
		err := redis.Delete(key)
		if err != nil {
			return err
		}
	}
	return nil
}
