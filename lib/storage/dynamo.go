package storage

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	awsdynamo "github.com/aws/aws-sdk-go/service/dynamodb"
)

type DynamoStorageManager struct {
	dataTable        string
	transactionTable string
	dynamoClient     *awsdynamo.DynamoDB
}

func NewDynamoStorageManager(dataTable string, transactionTable string) *DynamoStorageManager {
	dc := awsdynamo.New(session.New(), &aws.Config{
		Region: aws.String(endpoints.UsEast1RegionID),
	})

	return &DynamoStorageManager{
		dataTable:        dataTable,
		transactionTable: transactionTable,
		dynamoClient:     dc,
	}
}

func (dynamo *DynamoStorageManager) CommitTransaction(tid string, CommitTS string, writeBuffer map[string][]byte) error {
	writeSet := make([]string, 0)
	for key, val := range writeBuffer {
		newKey := fmt.Sprintf("%s%s%s", key, keyVersionDelim, CommitTS)
		err := dynamo.Put(newKey, val)
		writeSet = append(writeSet, newKey)
		if err != nil {
			return err
		}
	}
	byteWriteSet := _convertStringToBytes(writeSet)
	err := dynamo.Put(tid, byteWriteSet)
	return err
}


func (dynamo *DynamoStorageManager) Get(key string) ([]byte, error) {
	input := &awsdynamo.GetItemInput{
		Key:       *_constructKeyData(key),
		TableName: aws.String(dynamo.dataTable),
	}

	item, err := dynamo.dynamoClient.GetItem(input)
	if err != nil {
		return nil, err
	}

	for item == nil || item.Item == nil {
		item, err = dynamo.dynamoClient.GetItem(input)
	}

	bts := item.Item["Value"].B
	return bts, err
}

func (dynamo *DynamoStorageManager) Put(key string, val []byte) error {
	input := _constructPutInput(key, dynamo.dataTable, val)
	_, err := dynamo.dynamoClient.PutItem(input)
	return err
}


func (dynamo *DynamoStorageManager) MultiPut(keys []string, vals [][]byte) error {
	inputData := map[string][]*awsdynamo.WriteRequest{}
	inputData[dynamo.dataTable] = []*awsdynamo.WriteRequest{}

	numWrites := 0
	for index, key := range keys {
		valPerKey := vals[index]

		keyData := map[string]*awsdynamo.AttributeValue{
			"Key": {
				S: aws.String(key),
			},
			"Value": {
				B: valPerKey,
			},
		}

		putReq := &awsdynamo.PutRequest{Item: keyData}
		inputData[dynamo.dataTable] = append(inputData[dynamo.dataTable], &awsdynamo.WriteRequest{PutRequest: putReq})

		// DynamoDB's BatchWriteItem only supports 25 writes at a time, so if we
		// have more than that, we break it up.
		numWrites += 1
		if numWrites == 25 {
			_, err := dynamo.dynamoClient.BatchWriteItem(&awsdynamo.BatchWriteItemInput{RequestItems: inputData})
			if err != nil {
				return err
			}

			inputData = map[string][]*awsdynamo.WriteRequest{}
			inputData[dynamo.dataTable] = []*awsdynamo.WriteRequest{}
			numWrites = 0
		}
	}

	if len(inputData[dynamo.dataTable]) > 0 {
		_, err := dynamo.dynamoClient.BatchWriteItem(&awsdynamo.BatchWriteItemInput{RequestItems: inputData})
		return err
	}

	return nil
}


func (dynamo *DynamoStorageManager) GetTransactionWriteSet(transactionKey string) ([]string, error) {
	input := &awsdynamo.GetItemInput{
		Key:       *_constructKeyData(transactionKey),
		TableName: aws.String(dynamo.transactionTable),
	}

	item, err := dynamo.dynamoClient.GetItem(input)
	if err != nil {
		return nil, err
	}

	bytesValue := item.Item["Value"].B
	writeSet := _convertBytesToString(bytesValue)
	return writeSet, err
}

func (dynamo *DynamoStorageManager) MultiGetTransactionWriteSet(transactionKeys *[]string) (*[][]string, error) {
	results := make([][]string, len(*transactionKeys))
	resultIndex := 0

	input := map[string]*awsdynamo.KeysAndAttributes{}
	input[dynamo.dataTable] = &awsdynamo.KeysAndAttributes{}
	input[dynamo.dataTable].Keys = []map[string]*awsdynamo.AttributeValue{}

	for _, key := range *transactionKeys {
		keyData := *_constructKeyData(key)
		input[dynamo.dataTable].Keys = append(input[dynamo.dataTable].Keys, keyData)
		if len(input[dynamo.dataTable].Keys) == 100 {
			err := dynamo._helperGetBatch(&input, &results, &resultIndex)
			if err != nil {
				return nil, err
			}
			input = map[string]*awsdynamo.KeysAndAttributes{}
			input[dynamo.dataTable] = &awsdynamo.KeysAndAttributes{}
			input[dynamo.dataTable].Keys = []map[string]*awsdynamo.AttributeValue{}
		}
	}

	err := dynamo._helperGetBatch(&input, &results, &resultIndex)
	if err != nil {
		return nil, err
	}
	return &results, nil
}

func (dynamo *DynamoStorageManager) Delete(key string) error {
	input := &awsdynamo.DeleteItemInput{
		Key:       *_constructKeyData(key),
		TableName: aws.String(dynamo.dataTable),
	}
	_, err := dynamo.dynamoClient.DeleteItem(input)
	return err
}

func (dynamo *DynamoStorageManager) MultiDelete(keys *[]string) error {
	inputData := map[string][]*awsdynamo.WriteRequest{}
	inputData[dynamo.dataTable] = []*awsdynamo.WriteRequest{}

	numDeletes := 0
	for _, key := range *keys {
		keyData := _constructKeyData(key)
		deleteReq := &awsdynamo.DeleteRequest{Key: *keyData}
		inputData[dynamo.dataTable] = append(inputData[dynamo.dataTable], &awsdynamo.WriteRequest{DeleteRequest: deleteReq})
		numDeletes += 1
		if numDeletes == 25 {
			_, err := dynamo.dynamoClient.BatchWriteItem(&awsdynamo.BatchWriteItemInput{RequestItems: inputData})
			if err != nil {
				return err
			}
			inputData = map[string][]*awsdynamo.WriteRequest{}
			inputData[dynamo.dataTable] = []*awsdynamo.WriteRequest{}
			numDeletes = 0
		}
	}
	_, err := dynamo.dynamoClient.BatchWriteItem(&awsdynamo.BatchWriteItemInput{RequestItems: inputData})
	return err
}

// Helper Functions
func _constructKeyData(key string) *map[string]*awsdynamo.AttributeValue {
	return &map[string]*awsdynamo.AttributeValue{
		"Key": {
			S: aws.String(key),
		},
	}
}

func _constructPutInput(key string, table string, data []byte) *awsdynamo.PutItemInput {
	return &awsdynamo.PutItemInput{
		Item: map[string]*awsdynamo.AttributeValue{
			"Key": {
				S: aws.String(key),
			},
			"Value": {
				B: data,
			},
		},
		TableName: aws.String(table),
	}
}

func (dynamo *DynamoStorageManager) _helperGetBatch(input *map[string]*awsdynamo.KeysAndAttributes, results *[][]string, resultIndex *int) (error){
	response, err := dynamo.dynamoClient.BatchGetItem(&awsdynamo.BatchGetItemInput{RequestItems: *input})
	if err != nil {
		return err
	}

	for _, serialized := range response.Responses[dynamo.dataTable] {
		serializedData := serialized["Value"].B
		writeSet := make([]string, len(serializedData))
		for index, elem := range serializedData {
			writeSet[index] = string(elem)
		}
		(*results)[(*resultIndex)] = writeSet
		*resultIndex++
	}
	return nil
}