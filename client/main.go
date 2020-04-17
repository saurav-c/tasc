package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	awsdynamo "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/montanaflynn/stats"
	"google.golang.org/grpc"

	pb "github.com/saurav-c/aftsi/proto/aftsi/api"
)

var numRequests = flag.Int("numRequests", 1000, "The total number of requests in the benchmark")
var numWrites = flag.Int("numWrites", 1, "The total number of writes per request")
var address = flag.String("address", "", "The Aft replica to target")
var benchmarkType = flag.String("benchmarkType", "", "The type of benchmark to run: aft, aft-batch, batch, or direct")

func main() {
	flag.Parse() // Parse command line flags.

	if len(*address) == 0 {
		fmt.Println("Must provide Aft replica address.")
		os.Exit(1)
	}
	if len(*benchmarkType) == 0 {
		fmt.Println("Must provide benchmarkType: aft or direct.")
		os.Exit(1)
	}

	var latencies []float64
	var writeLatencies []float64
	if *benchmarkType == "aft" {
		latencies, writeLatencies = runAft(*numRequests, *address, *numWrites)

		median, _ := stats.Median(writeLatencies)
		fifth, _ := stats.Percentile(writeLatencies, 5.0)
		nfifth, _ := stats.Percentile(writeLatencies, 95.0)
		first, _ := stats.Percentile(writeLatencies, 1.0)
		nninth, _ := stats.Percentile(writeLatencies, 99.0)

		fmt.Println("WRITE LATENCIES:")
		fmt.Printf("\tMedian latency: %.6f\n", median)
		fmt.Printf("\t5th percentile/95th percentile: %.6f, %.6f\n", fifth, nfifth)
		fmt.Printf("\t1st percentile/99th percentile: %.6f, %.6f\n", first, nninth)
	} else if *benchmarkType == "direct" {
		latencies = runDirect(*numRequests, *numWrites)
	} else {
		fmt.Printf("Unknown benchmark type: %s", *benchmarkType)
	}

	median, _ := stats.Median(latencies)
	fifth, _ := stats.Percentile(latencies, 5.0)
	nfifth, _ := stats.Percentile(latencies, 95.0)
	first, _ := stats.Percentile(latencies, 1.0)
	nninth, _ := stats.Percentile(latencies, 99.0)

	fmt.Println("E2E LATENCIES:")
	fmt.Printf("\tMedian latency: %.6f\n", median)
	fmt.Printf("\t5th percentile/95th percentile: %.6f, %.6f\n", fifth, nfifth)
	fmt.Printf("\t1st percentile/99th percentile: %.6f, %.6f\n", first, nninth)
}

func runAft(numRequests int, address string, numWrites int) ([]float64, []float64) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:5000", address), grpc.WithInsecure())
	if err != nil {
		fmt.Printf("Unexpected error:\n%v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewAftSIClient(conn)

	latencies := make([]float64, numRequests)
	writeLatencies := make([]float64, numRequests)

	writeData := make([]byte, 4096)
	rand.Read(writeData)

	for i := 0; i < numRequests; i++ {
		txnStart := time.Now()
		txn, _ := client.StartTransaction(context.TODO(), &empty.Empty{})
		txnid := txn.GetTid()
		writeStart := time.Now()
		for j := 0; j < numWrites; j++ {
			//writeValue := fmt.Sprintf("%s-%d", "Hello World", j)
			//update := &pb.WriteRequest{Tid: txnid, Key: "a", Value: []byte(writeValue)}
			read := &pb.ReadRequest{
				Tid: txnid,
				Key: strconv.Itoa(j),
			}
			client.Read(context.TODO(), read)
		}
		writeEnd := time.Now()

		//resp, err := client.CommitTransaction(context.TODO(), &pb.TransactionID{Tid: txnid})
		txnEnd := time.Now()

		writeLatencies[i] = writeEnd.Sub(writeStart).Seconds()
		latencies[i] = txnEnd.Sub(txnStart).Seconds()

		//if err != nil || resp.E != pb.TransactionError_SUCCESS {
		//	fmt.Printf("Error!\n%v\n", err)
		//}
	}
	return latencies, writeLatencies
}

func runAft1(numRequests int, address string, numWrites int) ([]float64, []float64) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:5000", address), grpc.WithInsecure())
	if err != nil {
		fmt.Printf("Unexpected error:\n%v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewAftSIClient(conn)

	latencies := make([]float64, numRequests)
	writeLatencies := make([]float64, numRequests)

	writeData := make([]byte, 4096)
	rand.Read(writeData)

	for i := 0; i < numRequests; i++ {
		txnStart := time.Now()
		txn, _ := client.StartTransaction(context.TODO(), &empty.Empty{})
		txnid := txn.GetTid()
		writeStart := time.Now()
		for j := 0; j < numWrites; j++ {
			writeValue := fmt.Sprintf("%s-%d", "Hello World", j)
			update := &pb.WriteRequest{Tid: txnid, Key: "a", Value: []byte(writeValue)}
			client.Write(context.TODO(), update)
		}
		writeEnd := time.Now()

		resp, err := client.CommitTransaction(context.TODO(), &pb.TransactionID{Tid: txnid})
		txnEnd := time.Now()

		writeLatencies[i] = writeEnd.Sub(writeStart).Seconds()
		latencies[i] = txnEnd.Sub(txnStart).Seconds()

		if err != nil || resp.E != pb.TransactionError_SUCCESS {
			fmt.Printf("Error!\n%v\n", err)
		}
	}
	return latencies, writeLatencies
}

func runDirect1(numRequests int, numWrites int) []float64 {
	dc := awsdynamo.New(session.New(), &aws.Config{
		Region: aws.String(endpoints.UsEast1RegionID),
	})

	latencies := make([]float64, numRequests)
	writeData := make([]byte, 4096)
	rand.Read(writeData)

	input := &awsdynamo.PutItemInput{
		Item: map[string]*awsdynamo.AttributeValue{
			"Key": {
				S: aws.String("a"),
			},
			"Value": {
				B: writeData,
			},
		},
		TableName: aws.String("Aftsi"),
	}

	for i := 0; i < numRequests; i++ {
		txnStart := time.Now()
		for j := 0; j < numWrites; j++ {
			_, err := dc.PutItem(input)

			if err != nil {
				fmt.Printf("Error!\n%v\n", err)
			}
		}

		txnEnd := time.Now()
		latencies[i] = txnEnd.Sub(txnStart).Seconds()
	}

	return latencies
}

func addWrites(numRequests int, numWrites int) []float64 {
	dc := awsdynamo.New(session.New(), &aws.Config{
		Region: aws.String(endpoints.UsEast1RegionID),
	})

	latencies := make([]float64, numRequests)
	writeData := make([]byte, 4096)
	rand.Read(writeData)



	for i := 0; i < numRequests; i++ {
		input := &awsdynamo.PutItemInput{
			Item: map[string]*awsdynamo.AttributeValue{
				"Key": {
					S: aws.String(strconv.Itoa(i) + ":0"),
				},
				"Value": {
					B: writeData,
				},
			},
			TableName: aws.String("Aftsi"),
		}

		inputR := &awsdynamo.GetItemInput{
			Key:                    map[string]*awsdynamo.AttributeValue{
				"Key": {
					S: aws.String(strconv.Itoa(i) + ":0"),
				},
			},
			TableName:              aws.String("Aftsi"),
		}

		txnStart := time.Now()
		for j := 0; j < numWrites; j++ {
			_, err := dc.PutItem(input)
			dc.GetItem(inputR)

			if err != nil {
				fmt.Printf("Error!\n%v\n", err)
			}
		}

		txnEnd := time.Now()
		latencies[i] = txnEnd.Sub(txnStart).Seconds()
	}

	return latencies
}

func runDirect(numRequests int, numWrites int) []float64 {
	dc := awsdynamo.New(session.New(), &aws.Config{
		Region: aws.String(endpoints.UsEast1RegionID),
	})

	latencies := make([]float64, numRequests)
	writeData := make([]byte, 4096)
	rand.Read(writeData)



	for i := 0; i < numRequests; i++ {
		//input := &awsdynamo.PutItemInput{
		//	Item: map[string]*awsdynamo.AttributeValue{
		//		"Key": {
		//			S: aws.String(strconv.Itoa(i) + ":0"),
		//		},
		//		"Value": {
		//			B: writeData,
		//		},
		//	},
		//	TableName: aws.String("Aftsi"),
		//}

		input := &awsdynamo.GetItemInput{
			Key:                    map[string]*awsdynamo.AttributeValue{
				"Key": {
					S: aws.String(strconv.Itoa(i) + ":0"),
				},
			},
			TableName:              aws.String("Aftsi"),
		}

		txnStart := time.Now()
		for j := 0; j < numWrites; j++ {
			dc.GetItem(input)

			//if err != nil {
			//	fmt.Printf("Error!\n%v\n", err)
			//}
		}

		txnEnd := time.Now()
		latencies[i] = txnEnd.Sub(txnStart).Seconds()
	}

	return latencies
}