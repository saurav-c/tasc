package main

import (
	"context"
	"flag"
	"fmt"
	cmn "github.com/saurav-c/tasc/lib/common"
	"github.com/saurav-c/tasc/lib/storage"
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
var numWrites = flag.Int("numWrites", 1000, "The number of writes to do")
var numReads = flag.Int("numReads", 2, "The number of reads to do per transaction")
var benchmarkType = flag.String("type", "", "The type of benchmark to run: "+benchmarks)
var numRequests = flag.Int("numReq", 1000, "Number of requests to run")
var numThreads = flag.Int("numThreads", 10, "The total number of parallel threads in the benchmark")
var annaELB = flag.String("anna", "", "Anna ELB Address")

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
				go throughputPerClient(i, *address, *numRequests, *numWrites, *numReads, latency, totalTimeChannel, false)
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
	case "dynamo":
		{
			latencies := runDynamo(*numRequests, *numWrites, *numReads)
			printLatencies(latencies, "End to End Latencies")
		}
	case "anna":
		{
			latencies := runAnna(*numRequests, *numWrites, *numReads, *annaELB, *address)
			printLatencies(latencies, "End to End Latencies")
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
	err = sckt.Connect(fmt.Sprintf("tcp://%s:%d", elbEndpoint, cmn.LoadBalancerPort))
	if err != nil {
		return "", err
	}
	defer sckt.Close()
	sckt.SendBytes(nil, zmq.DONTWAIT)
	txnAddressBytes, _ := sckt.RecvBytes(0)
	return string(txnAddressBytes), nil
}

func throughputPerClient(
	uniqueThread int,
	address string,
	numReq int,
	numWrites int,
	numReads int,
	latency chan []float64,
	totalTime chan float64,
	useELB bool) {
	txnManagerAddr := address
	if useELB {
		addr, err := getTASCClientAddr(address)
		txnManagerAddr = addr
		if err != nil {
			fmt.Printf("Unexpected error:\n%v\n", err)
			os.Exit(1)
		}
	}
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", txnManagerAddr, cmn.TxnManagerServerPort), grpc.WithInsecure())
	if err != nil {
		fmt.Printf("Unexpected error:\n%v\n", err)
		os.Exit(1)
	}
	client := pb.NewTascClient(conn)

	latencies := make([]float64, 0)
	writeData := make([]byte, 4096)
	rand.Read(writeData)

	totalBench := float64(0)
	for i := 0; i < numReq; i++ {
		totalTime := float64(0)
		txn, _ := client.StartTransaction(context.TODO(), &empty.Empty{})
		tid := txn.GetTid()
		for j := 0; j < numWrites; j++ {
			key := fmt.Sprintf("tascThroughput-%d-%d-%d", uniqueThread, i, j)
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
			start := time.Now()
			_, err := client.Write(context.TODO(), write)
			end := time.Now()
			if err != nil {
				fmt.Println("Writes are failing")
				fmt.Println(err)
			} else {
				totalTime += end.Sub(start).Seconds()
			}
		}
		for j := 0; j < numReads; j++ {
			readKey := j
			if numWrites > 0 {
				readKey = rand.Intn(numWrites)
			}
			key := fmt.Sprintf("tascThroughput-%d-%d-%d", uniqueThread, i, readKey)
			reads := []*pb.TascRequest_KeyPair{
				{
					Key: key,
				},
			}
			read := &pb.TascRequest{
				Tid:   tid,
				Pairs: reads,
			}
			start := time.Now()
			_, err = client.Read(context.TODO(), read)
			end := time.Now()
			if err != nil {
				fmt.Println("Reads are failing")
				fmt.Println(err)
			} else {
				totalTime += end.Sub(start).Seconds()
			}
		}
		start := time.Now()
		resp, err := client.CommitTransaction(context.TODO(), &pb.TransactionTag{
			Tid: tid,
		})
		end := time.Now()
		if resp.Status != pb.TascTransactionStatus_COMMITTED {
			fmt.Println("Commit failed")
			fmt.Println(err)
		}
		totalTime += end.Sub(start).Seconds()
		latencies = append(latencies, totalTime)
		totalBench += totalTime
	}
	latency <- latencies
	totalTime <- totalBench
}

/*
* FUNCTIONS FOR TESTING DYNAMODB THROUGHPUT/WRITE LATENCY
 */

func runDynamo(numRequests int, numWrites int, numReads int) []float64 {
	dc := awsdynamo.New(session.New(), &aws.Config{
		Region: aws.String(endpoints.UsEast1RegionID),
	})

	latencies := []float64{}
	writeData := make([]byte, 4096)
	rand.Read(writeData)

	for i := 0; i < numRequests; i++ {
		start := time.Now()
		var inputs []*awsdynamo.WriteRequest
		for j := 0; j < numWrites; j++ {
			input := &awsdynamo.PutRequest{
				Item: map[string]*awsdynamo.AttributeValue{
					"Key": {
						S: aws.String("dynamo" + string(i) + string(j)),
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
		if numWrites > 0 {
			writeReq := make(map[string][]*awsdynamo.WriteRequest)
			writeReq["Aftsi-Benchmark"] = inputs
			_, err := dc.BatchWriteItem(&awsdynamo.BatchWriteItemInput{
				RequestItems: writeReq,
			})
			if err != nil {
				fmt.Println(err)
				panic("Error writing to DynamoDB")
			}
		}

		for j := 0; j < numReads; j++ {
			key := map[string]*awsdynamo.AttributeValue{
				"DataKey": {
					S: aws.String("dynamo" + string(i) + string(j)),
				},
			}
			input := &awsdynamo.GetItemInput{
				Key:       key,
				TableName: aws.String("Aftsi-Benchmark"),
			}
			_, err := dc.GetItem(input)
			if err != nil {
				fmt.Println(err)
				panic("Error reading from DynamoDB")
			}
		}
		end := time.Now()
		latencies = append(latencies, end.Sub(start).Seconds())
	}
	return latencies
}

func runAnna(numRequests int, numWrites int, numReads int, elb string, myAddr string) []float64 {
	latencies := []float64{}
	annaClient := storage.NewAnnaClient(elb, myAddr, false, 0)

	for i := 0; i < numRequests; i++ {
		writeData := make([]byte, 4096)
		rand.Read(writeData)

		start := time.Now()
		// Writes
		keys := []string{}
		for j := 0; j < numWrites; j++ {
			key := fmt.Sprintf("key-%d-%d", i, j)
			keys = append(keys, key)
			annaClient.Put(key, writeData)
		}

		// Reads
		for j := 0; j < numReads; j++ {
			key := fmt.Sprintf("key-%d-%d", i, j)
			annaClient.Get(key)
		}
		end := time.Now()
		latencies = append(latencies, end.Sub(start).Seconds())
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