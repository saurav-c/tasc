package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	awsdynamo "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/montanaflynn/stats"
	"google.golang.org/grpc"

	pb "github.com/saurav-c/aftsi/proto/aftsi/api"
	rtr "github.com/saurav-c/aftsi/proto/routing/api"
)

const (
	benchmarks = "aftsiWrites, aftsiNoRouterWrites dynamoWrites, dynamoBatchWrites"
)

var address = flag.String("address", "", "The Transaction Manager")
var benchmarkType = flag.String("type", "", "The type of benchmark to run: " + benchmarks)
var numRequests = flag.Int("numReq", 1000, "Number of requests to run")
var rtrAddr = flag.String("rtr", "", "Txn Router Address")

func main() {
	flag.Parse()
	if len(*benchmarkType) == 0 {
		fmt.Println("Must provide a benchmarkType: " + benchmarks)
	}

	switch *benchmarkType {
	case "aftsiWrites":
		{
			latencies, writeLatencies := runAftsiWrites(*rtrAddr, *address, *numRequests)
			printLatencies(latencies, "End to End Latencies")
			printLatencies(writeLatencies, "Write Latencies")
		}
	case "aftsiNRTRWrites":
		{
			latencies, writeLatencies := runAftsiNoRouterWrites(*address, *numRequests)
			printLatencies(latencies, "End to End Latencies")
			printLatencies(writeLatencies, "Write Latencies")
		}
	case "dynamoWrites":
		{
			latencies := runDynamoWrites(*numRequests)
			printLatencies(latencies, "End to End Latencies")
		}
	case "dynamoBatchWrites":
		{
			latencies := runDynamoBatchWrites(*numRequests)
			printLatencies(latencies, "End to End Latencies")
		}
	}
}

func runAftsiWrites(routerAddr string, defaultTxn string, numReq int) (map[int][]float64, map[int][]float64) {
	// Establish connection with Router
	conn, err := grpc.Dial(fmt.Sprintf("%s:5006", routerAddr), grpc.WithInsecure())
	if err != nil {
		fmt.Printf("Unexpected error:\n%v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := rtr.NewRouterClient(conn)

	latencies := make(map[int][]float64, 3)
	writeLatencies := make(map[int][]float64, 3)

	writeData := make([]byte, 4096)
	rand.Read(writeData)

	clientConns := make(map[string]pb.AftSIClient)

	// Make the default connection to a Txn Manager
	tConn, err := grpc.Dial(fmt.Sprintf("%s:5000", defaultTxn), grpc.WithInsecure())
	if err != nil {
		fmt.Printf("Unexpected error:\n%v\n", err)
		os.Exit(1)
	}
	defer tConn.Close()
	clientConns[defaultTxn] = pb.NewAftSIClient(tConn)

	for _, numWrites := range []int{1, 5, 10} {
		for i := 0; i < numReq; i++ {
			txnStart := time.Now()
			txn, _ := clientConns[defaultTxn].StartTransaction(context.TODO(), &empty.Empty{})
			tid := txn.GetTid()

			// Get Ip Addr of this txns manager
			rtrResp, _ := client.LookUp(context.TODO(), &rtr.RouterReq{
				Req: tid,
			})
			managerIP := rtrResp.GetIp()
			if _, ok := clientConns[managerIP]; !ok {
				c, err := grpc.Dial(fmt.Sprintf("%s:5000", managerIP), grpc.WithInsecure())
				if err != nil {
					fmt.Printf("Unexpected error:\n%v\n", err)
					os.Exit(1)
				}
				defer c.Close()
				clientConns[managerIP] = pb.NewAftSIClient(c)
			}

			txnConn := clientConns[managerIP]

			writeStart := time.Now()
			for j := 0; j < numWrites; j++ {
				key := fmt.Sprintf("aftsiWrite-%s-%s-%s", string(numWrites), string(i), string(j))
				write := &pb.WriteRequest{
					Tid:   tid,
					Key:   key,
					Value: writeData,
				}
				txnConn.Write(context.TODO(), write)
			}
			writeEnd := time.Now()
			resp, _ := txnConn.CommitTransaction(context.TODO(), &pb.TransactionID{
				Tid: tid,
			})
			txnEnd := time.Now()
			if resp.GetE() != pb.TransactionError_SUCCESS {
				panic("Commit failed")
			}
			writeLatencies[numWrites] = append(writeLatencies[numWrites], 1000 * writeEnd.Sub(writeStart).Seconds())
			latencies[numWrites] = append(latencies[numWrites], 1000 * txnEnd.Sub(txnStart).Seconds())
		}
	}
	return latencies, writeLatencies
}

func runAftsiNoRouterWrites(txnManagerAddr string, numReq int) (map[int][]float64, map[int][]float64) {
	// Establish connection
	conn, err := grpc.Dial(fmt.Sprintf("%s:5000", txnManagerAddr), grpc.WithInsecure())
	if err != nil {
		fmt.Printf("Unexpected error:\n%v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := pb.NewAftSIClient(conn)

	latencies := make(map[int][]float64, 3)
	writeLatencies := make(map[int][]float64, 3)

	writeData := make([]byte, 4096)
	rand.Read(writeData)

	for _, numWrites := range []int{1, 5, 10} {
		for i := 0; i < numReq; i++ {
			txnStart := time.Now()
			txn, _ := client.StartTransaction(context.TODO(), &empty.Empty{})
			tid := txn.GetTid()
			writeStart := time.Now()
			for j := 0; j < numWrites; j++ {
				key := fmt.Sprintf("aftsiWrite-%s-%s-%s", string(numWrites), string(i), string(j))
				write := &pb.WriteRequest{
					Tid:   tid,
					Key:   key,
					Value: writeData,
				}
				client.Write(context.TODO(), write)
			}
			writeEnd := time.Now()
			resp, _ := client.CommitTransaction(context.TODO(), &pb.TransactionID{
				Tid: tid,
			})
			txnEnd := time.Now()
			if resp.GetE() != pb.TransactionError_SUCCESS {
				panic("Commit failed")
			}
			writeLatencies[numWrites] = append(writeLatencies[numWrites], 1000 * writeEnd.Sub(writeStart).Seconds())
			latencies[numWrites] = append(latencies[numWrites], 1000 * txnEnd.Sub(txnStart).Seconds())
		}
	}
	return latencies, writeLatencies
}

func runDynamoWrites(numRequests int) (map[int][]float64) {

	dc := awsdynamo.New(session.New(), &aws.Config{
		Region: aws.String(endpoints.UsEast1RegionID),
	})

	latencies := make(map[int][]float64, numRequests)
	writeData := make([]byte, 4096)
	rand.Read(writeData)

	for _, numWrites := range []int{1, 5, 10} {
		for i := 0; i < numRequests; i++ {
			start := time.Now()
			for j := 0; j < numWrites; j++ {
				input := &awsdynamo.PutItemInput{
					Item: map[string]*awsdynamo.AttributeValue{
						"Key": {
							S: aws.String("direct" + string(numWrites) + string(i) + string(j)),
						},
						"Value": {
							B: writeData,
						},
					},
					TableName: aws.String("Aftsi-Benchmark"),
				}
				_, err := dc.PutItem(input)
				if err != nil {
					fmt.Println(err)
					panic("Error writing to Dynamo")
				}
			}
			end := time.Now()
			latencies[numWrites] = append(latencies[numWrites], 1000 * end.Sub(start).Seconds())
		}
	}
	return latencies
}

func runDynamoBatchWrites(numRequests int) (map[int][]float64) {
	dc := awsdynamo.New(session.New(), &aws.Config{
		Region: aws.String(endpoints.UsEast1RegionID),
	})

	latencies := make(map[int][]float64, numRequests)
	writeData := make([]byte, 4096)
	rand.Read(writeData)


	for _, numWrites := range []int{1, 5, 10} {
		for i := 0; i < numRequests; i++ {
			start := time.Now()

			var inputs []*awsdynamo.WriteRequest
			for j := 0; j < numWrites; j++ {
				input := &awsdynamo.PutRequest{
					Item: map[string]*awsdynamo.AttributeValue{
						"Key": {
							S: aws.String("directBatch" + string(numWrites) + string(i) + string(j)),
						},
						"Value": {
							B: writeData,
						},
					},
				}
				inputs = append(inputs, &awsdynamo.WriteRequest{
					PutRequest: input,
				})
			}
			writeReq := make(map[string][]*awsdynamo.WriteRequest)
			writeReq["Aftsi-Benchmark"] = inputs
			_, err := dc.BatchWriteItem(&awsdynamo.BatchWriteItemInput{
				RequestItems: writeReq,
			})
			end := time.Now()
			if err != nil {
				fmt.Println(err)
				panic("Error writing to DynamoDB")
			}

			latencies[numWrites] = append(latencies[numWrites], 1000 * end.Sub(start).Seconds())
		}
	}
	return latencies
}

func printLatencies(latencies map[int][]float64, title string) {
	fmt.Println(title)
	for k, lats := range latencies {
		fmt.Printf("Number of Writes: %d", k)
		median, _ := stats.Median(lats)
		fifth, _ := stats.Percentile(lats, 5.0)
		nfifth, _ := stats.Percentile(lats, 95.0)
		first, _ := stats.Percentile(lats, 1.0)
		nninth, _ := stats.Percentile(lats, 99.0)
		fmt.Printf("\tMedian latency: %.6f\n", median)
		fmt.Printf("\t5th percentile/95th percentile: %.6f, %.6f\n", fifth, nfifth)
		fmt.Printf("\t1st percentile/99th percentile: %.6f, %.6f\n", first, nninth)
		fmt.Println()
	}
}



