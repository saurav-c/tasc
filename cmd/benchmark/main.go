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
	zmq "github.com/pebbe/zmq4"
	"google.golang.org/grpc"

	pb "github.com/saurav-c/tasc/proto/tasc"
)

const (
	benchmarks = "tascWrites, throughput, keyNodeNum, dynamoWrites, dynamoBatchWrites"
)

var address = flag.String("address", "", "ELB Address")
var benchmarkType = flag.String("type", "", "The type of benchmark to run: "+benchmarks)
var numRequests = flag.Int("numReq", 1000, "Number of requests to run")
var numThreads = flag.Int("numThreads", 10, "The total number of parallel threads in the benchmark")

func main() {
	flag.Parse()
	if len(*benchmarkType) == 0 {
		fmt.Println("Must provide a benchmarkType: " + benchmarks)
	}

	switch *benchmarkType {
	case "throughput":
		{
			latency := make(chan []float64)
			totalTimeChannel := make(chan float64)

			for i := 0; i < *numThreads; i++ {
				go throughputPerClient(i, *address, *numRequests, 4, 2, latency, totalTimeChannel)
			}

			latencies := []float64{}
			throughputs := []float64{}

			for tid := 0; tid < *numThreads; tid++ {
				latencyArray := <-latency
				latencies = append(latencies, latencyArray...)

				threadTime := <-totalTimeChannel
				throughputs = append(throughputs, float64(*numRequests)/threadTime)
			}

			printLatencies(latencies, "End to End Latencies")
			printThroughput(throughputs, *numThreads, "Throughput of the system")
		}
	case "keyNodeNum":
		{
			l1 := keyNodeWriteTest(*address, *numRequests, []string{"apple"})
			//l2 := keyNodeWriteTest(*address, *numRequests, []string{"apple", "ban"})
			//l3 := keyNodeWriteTest(*address, *numRequests, []string{"apple", "ban", "goo"})
			//l4 := keyNodeWriteTest(*address, *numRequests, []string{"apple", "goo", "banana", "sauravchh"})

			printLatencies(l1, "1 Key Node")
			//printLatencies(l2, "2 Key Nodes")
			//printLatencies(l3, "3 Key Nodes")
			//printLatencies(l4, "4 Key Nodes")
		}
	case "tascWrites":
		{
			latencies, writeLatencies := runTascWrites(*address, *numRequests)
			printWriteLatencies(latencies, "End to End Latencies")
			printWriteLatencies(writeLatencies, "Write Latencies")
		}
	case "dynamoWrites":
		{
			latencies := runDynamoWrites(*numRequests)
			printWriteLatencies(latencies, "End to End Latencies")
		}
	case "dynamoBatchWrites":
		{
			latencies := runDynamoBatchWrites(*numRequests)
			printWriteLatencies(latencies, "End to End Latencies")
		}
	}
}

func getTASCClientAddr(elbEndpoint string) (txnAddress string, err error) {
	zctx, err := zmq.NewContext()
	if err != nil {
		return "", err
	}
	sckt, err := zctx.NewSocket(zmq.REQ)
	if err != nil {
		return "", err
	}
	err = sckt.Connect(fmt.Sprintf("tcp://%s:8000", elbEndpoint))
	if err != nil {
		return "", err
	}
	defer sckt.Close()
	sckt.SendBytes(nil, zmq.DONTWAIT)
	txnAddressBytes, _ := sckt.RecvBytes(0)
	return string(txnAddressBytes), nil
}

func runTascWrites(elbAddr string, numReq int) (map[int][]float64, map[int][]float64) {
	// Establish connection
	txnManagerAddr, err := getTASCClientAddr(elbAddr)
	conn, err := grpc.Dial(fmt.Sprintf("%s:5000", txnManagerAddr), grpc.WithInsecure())
	if err != nil {
		fmt.Printf("Unexpected error:\n%v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := pb.NewTascClient(conn)

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
				key := fmt.Sprintf("tascWrite-%s-%s-%s", string(numWrites), string(i), string(j))
				writes := []*pb.TascRequest_KeyPair{
					{
						Key: key,
						Value: writeData,
					},
				}
				write := &pb.TascRequest{
					Tid:   tid,
					Pairs: writes,
				}
				client.Write(context.TODO(), write)
			}
			writeEnd := time.Now()
			resp, _ := client.CommitTransaction(context.TODO(), &pb.TransactionTag{
				Tid: tid,
			})
			txnEnd := time.Now()
			if resp.Status != pb.TascTransactionStatus_COMMITTED {
				panic("Commit failed")
			}
			writeLatencies[numWrites] = append(writeLatencies[numWrites], 1000*writeEnd.Sub(writeStart).Seconds())
			latencies[numWrites] = append(latencies[numWrites], 1000*txnEnd.Sub(txnStart).Seconds())
		}
	}
	return latencies, writeLatencies
}

func throughputPerClient(
	uniqueThread int,
	elbEndpoint string,
	numReq int,
	numWrites int,
	numReads int,
	latency chan []float64,
	totalTime chan float64) {
	txnManagerAddr, err := getTASCClientAddr(elbEndpoint)
	if err != nil {
		fmt.Printf("Unexpected error:\n%v\n", err)
		os.Exit(1)
	}
	conn, err := grpc.Dial(fmt.Sprintf("%s:5000", txnManagerAddr), grpc.WithInsecure())
	if err != nil {
		fmt.Printf("Unexpected error:\n%v\n", err)
		os.Exit(1)
	}
	client := pb.NewTascClient(conn)

	latencies := make([]float64, 0)

	benchStart := time.Now()
	writeData := make([]byte, 4096)
	rand.Read(writeData)

	for i := 0; i < numReq; i++ {
		txnStart := time.Now()
		txn, _ := client.StartTransaction(context.TODO(), &empty.Empty{})
		tid := txn.GetTid()
		for j := 0; j < numWrites; j++ {
			key := fmt.Sprintf("aftsiThroughput-%d-%d-%d", uniqueThread, i, j)
			writes := []*pb.TascRequest_KeyPair{
				{
					Key: key,
					Value: writeData,
				},
			}
			write := &pb.TascRequest{
				Tid:   tid,
				Pairs: writes,
			}
			_, err := client.Write(context.TODO(), write)
			if err != nil {
				fmt.Println("Writes are failing")
				fmt.Println(err)
			}
		}
		for j := 0; j < numReads; j++ {
			readKey := rand.Intn(numWrites)
			key := fmt.Sprintf("aftsiThroughput-%d-%d-%d", uniqueThread, i, readKey)
			reads := []*pb.TascRequest_KeyPair{
				{
					Key: key,
				},
			}
			read := &pb.TascRequest{
				Tid:   tid,
				Pairs: reads,
			}
			_, err = client.Read(context.TODO(), read)
			if err != nil {
				fmt.Println("Reads are failing")
				fmt.Println(err)
			}
		}
		resp, err := client.CommitTransaction(context.TODO(), &pb.TransactionTag{
			Tid: tid,
		})
		txnEnd := time.Now()
		if resp.Status != pb.TascTransactionStatus_COMMITTED {
			fmt.Println("Commit failed")
			fmt.Println(err)
		}
		latencies = append(latencies, txnEnd.Sub(txnStart).Seconds())
	}
	benchEnd := time.Now()
	latency <- latencies
	totalTime <- benchEnd.Sub(benchStart).Seconds()
}

func keyNodeWriteTest(elbEndpoint string, numReq int, keys []string) []float64 {
	// Establish connection
	txnManagerAddr, err := getTASCClientAddr(elbEndpoint)
	if err != nil {
		fmt.Printf("Unexpected error:\n%v\n", err)
		os.Exit(1)
	}
	conn, err := grpc.Dial(fmt.Sprintf("%s:5000", txnManagerAddr), grpc.WithInsecure())
	if err != nil {
		fmt.Printf("Unexpected error:\n%v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := pb.NewTascClient(conn)

	latencies := make([]float64, 1000)

	writeData := make([]byte, 4096)
	rand.Read(writeData)

	for i := 0; i < numReq; i++ {
		txnStart := time.Now()
		txn, _ := client.StartTransaction(context.TODO(), &empty.Empty{})
		tid := txn.GetTid()

		for _, k := range keys {
			writes := []*pb.TascRequest_KeyPair{
				{
					Key: k,
					Value: writeData,
				},
			}
			write := &pb.TascRequest{
				Tid:   tid,
				Pairs: writes,
			}
			client.Write(context.TODO(), write)
		}
		resp, _ := client.CommitTransaction(context.TODO(), &pb.TransactionTag{
			Tid: tid,
		})
		txnEnd := time.Now()
		if resp.Status != pb.TascTransactionStatus_COMMITTED {
			panic("Commit failed")
		}
		latencies[i] = 1000 * txnEnd.Sub(txnStart).Seconds()
	}

	return latencies
}

/*
* FUNCTIONS FOR TESTING DYNAMODB THROUGHPUT/WRITE LATENCY
 */

func runDynamoWrites(numRequests int) map[int][]float64 {

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
			latencies[numWrites] = append(latencies[numWrites], 1000*end.Sub(start).Seconds())
		}
	}
	return latencies
}

func runDynamoBatchWrites(numRequests int) map[int][]float64 {
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

			latencies[numWrites] = append(latencies[numWrites], 1000*end.Sub(start).Seconds())
		}
	}
	return latencies
}

/*
* HELPER FUNCTIONS FOR BENCHMARK METRICS
 */

func printWriteLatencies(latencies map[int][]float64, title string) {
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

func printLatencies(latencies []float64, title string) {
	fmt.Println(title)
	median, _ := stats.Median(latencies)
	fifth, _ := stats.Percentile(latencies, 5.0)
	nfifth, _ := stats.Percentile(latencies, 95.0)
	first, _ := stats.Percentile(latencies, 1.0)
	nninth, _ := stats.Percentile(latencies, 99.0)
	fmt.Printf("\tMedian latency: %.6f\n", median)
	fmt.Printf("\t5th percentile/95th percentile: %.6f, %.6f\n", fifth, nfifth)
	fmt.Printf("\t1st percentile/99th percentile: %.6f, %.6f\n", first, nninth)
	fmt.Println()
}

func printThroughput(throughput []float64, numClients int, title string) {
	fmt.Println(title)
	fmt.Printf("Number of Clients: %d", numClients)
	throughputVal, _ := stats.Sum(throughput)
	fmt.Printf("\tTotal Throughput: %.6f\n", throughputVal)
	fmt.Println()
}