package storage

import (
	"bytes"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
)

type S3StorageManager struct {
	bucket   string
	s3Client *awss3.S3
}

func NewS3StorageManager(bucket string) *S3StorageManager {
	s3c := awss3.New(session.New(), &aws.Config{
		Region: aws.String(endpoints.UsEast1RegionID),
	})

	return &S3StorageManager{bucket: bucket, s3Client: s3c}
}

func (s3 *S3StorageManager) CommitTransaction(tid string, CommitTS string, writeBuffer map[string][]byte) error {
	writeSet := make([]string, 0)
	for key, val := range writeBuffer {
		newKey := fmt.Sprintf("%s%s%s-%s", key, keyVersionDelim, CommitTS, tid)
		input := &awss3.PutObjectInput{
			Bucket: &s3.bucket,
			Key:    &newKey,
			Body:   bytes.NewReader(val),
		}

		_, err := s3.s3Client.PutObject(input)
		if err != nil {
			return err
		}
		writeSet = append(writeSet, newKey)
	}
	byteWriteSet := _convertStringToBytes(writeSet)
	input := &awss3.PutObjectInput{
		Bucket: &s3.bucket,
		Key:    &tid,
		Body:   bytes.NewReader(byteWriteSet),
	}

	_, err := s3.s3Client.PutObject(input)
	return err
}

func (s3 *S3StorageManager) Get(key string) ([]byte, error) {
	input := &awss3.GetObjectInput{
		Bucket: &s3.bucket,
		Key:    &key,
	}

	getObjectOutput, err := s3.s3Client.GetObject(input)
	if err != nil {
		return nil, err
	}

	body := new(bytes.Buffer)
	_, err = body.ReadFrom(getObjectOutput.Body)
	return body.Bytes(), err
}

func (s3 *S3StorageManager) GetTransaction(transactionKey string) ([]string, error) {
	input := &awss3.GetObjectInput{
		Bucket: &s3.bucket,
		Key:    &transactionKey,
	}

	getObjectOutput, err := s3.s3Client.GetObject(input)
	if err != nil {
		return nil, err
	}

	body := new(bytes.Buffer)
	_, err = body.ReadFrom(getObjectOutput.Body)
	allBytes := body.Bytes()
	return _convertBytesToString(allBytes), err
}

func (s3 *S3StorageManager) MultiGetTransaction(transactionKeys *[]string) (*[][]string, error) {
	results := make([][]string, len(*transactionKeys))

	for index, key := range *transactionKeys {
		txn, err := s3.GetTransaction(key)
		if err != nil {
			return nil, err
		}
		results[index] = txn
	}

	return &results, nil
}

func (s3 *S3StorageManager) Put(key string, val []byte) error {
	input := &awss3.PutObjectInput{
		Bucket: &s3.bucket,
		Key:    &key,
		Body:   bytes.NewReader(val),
	}
	_, err := s3.s3Client.PutObject(input)
	return err
}

func (s3 *S3StorageManager) MultiPut(keys []string, vals [][]byte) ([]string, error) {
	writtenKeys := make([]string, 0)
	for index, key := range keys {
		val := vals[index]
		err := s3.Put(key, val)
		if err != nil {
			return writtenKeys, err
		}
		writtenKeys = append(writtenKeys, key)
	}
	return writtenKeys, nil
}

func (s3 *S3StorageManager) Delete(key string) error {
	input := &awss3.DeleteObjectInput{
		Bucket: &s3.bucket,
		Key:    &key,
	}

	_, err := s3.s3Client.DeleteObject(input)
	return err
}

func (s3 *S3StorageManager) MultiDelete(keys *[]string) error {
	for _, key := range *keys {
		err := s3.Delete(key)
		if err != nil {
			return err
		}
	}
	return nil
}
