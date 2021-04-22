package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/service/lambda"
	cmn "github.com/saurav-c/tasc/lib/common"
	"math/rand"
	"os"
	"strings"
	"time"
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/montanaflynn/stats"
	zmq "github.com/pebbe/zmq4"
	"google.golang.org/grpc"

	pb "github.com/saurav-c/tasc/proto/tasc"
)

const (
	benchmarks = "tasc, nonTasc, tasc-local"
)

var address = flag.String("address", "", "ELB Address")
var numWrites = flag.Int("numWrites", 1000, "The number of writes to do")
var numReads = flag.Int("numReads", 2, "The number of reads to do per transaction")
var benchmarkType = flag.String("type", "", "The type of benchmark to run: " + benchmarks)
var numRequests = flag.Int("numReq", 1000, "Number of requests to run per thread")
var numThreads = flag.Int("numThreads", 10, "The total number of parallel threads in the benchmark")

func main() {
	flag.Parse()
	if len(*benchmarkType) == 0 {
		fmt.Println("Must provide a benchmarkType: " + benchmarks)
	}

	latency := make(chan []float64)
	errorChannel := make(chan []string)
	totalTimeChannel := make(chan float64)

	for i := 0; i < *numThreads; i++ {
		switch *benchmarkType {
		case "tasc":
			go tasc(*address, *numRequests, *numWrites, *numReads, latency, errorChannel, totalTimeChannel)
		case "nonTasc":
			go nonTasc(*address, *numRequests, *numWrites, *numReads, latency, errorChannel, totalTimeChannel)
		case "tasc-local":
			go tascLocal(i, *address, *numRequests, *numWrites, *numReads, latency, errorChannel, totalTimeChannel)
		}
	}

	latencies := []float64{}
	throughputs := []float64{}
	errors := []string{}

	for tid := 0; tid < *numThreads; tid++ {
		latencyArray := <-latency
		latencies = append(latencies, latencyArray...)

		errorArray := <- errorChannel
		errors = append(errors, errorArray...)

		threadTime := <-totalTimeChannel
		throughputs = append(throughputs, float64(*numRequests)/threadTime)
	}

	printLatencies(latencies, "End to End Latencies")
	printErrors(errors, "Errors occurred during benchmark")
	printThroughput(throughputs, *numThreads, "Throughput of the system")
}

/*
* FUNCTIONS FOR LAMBDA TESTING
 */

func tasc(elbEndpoint string,
	numRequests int,
	numWrites int,
	numReads int,
	latency chan []float64,
	errorsChannel chan[]string,
	totalTime chan float64) {

	latencies := make([]float64, 0)
	errors := make([]string, 0)

	lambdaClient := lambda.New(
		session.New(),
		&aws.Config{Region: aws.String(endpoints.UsEast1RegionID)},
	)

	type lambdaInput struct {
		Count   int    `json:"count"`
		Reads   int    `json:"reads"`
		Writes  int    `json:"writes"`
		elb     string `json:"elb"`
	}

	tascInput := lambdaInput{
		Count:   1,
		elb:     elbEndpoint,
		Reads:   numReads,
		Writes:  numWrites,
	}
	payload, _ := json.Marshal(&tascInput)

	input := &lambda.InvokeInput{
		FunctionName:   aws.String("tasc"),
		Payload:        payload,
		InvocationType: aws.String("RequestResponse"),
	}

	benchStart := time.Now()

	for i := 0; i < numRequests; i++ {
		lambdaStart := time.Now()
		response, err := lambdaClient.Invoke(input)
		lambdaEnd := time.Now()
		latencies = append(latencies, lambdaEnd.Sub(lambdaStart).Seconds())

		if err != nil {
			errors = append(errors, err.Error())
		} else {
			// Next, we try to parse the response.
			bts := string(response.Payload)

			if !strings.Contains(bts, "Success") {
				errors = append(errors, bts)
			}
		}
	}
	benchEnd := time.Now()
	latency <- latencies
	errorsChannel <- errors
	totalTime <- benchEnd.Sub(benchStart).Seconds()
}

func nonTasc(elbEndpoint string,
	numRequests int,
	numWrites int,
	numReads int,
	latency chan []float64,
	errorsChannel chan[]string,
	totalTime chan float64) {

	latencies := make([]float64, 0)
	errors := make([]string, 0)

	lambdaClient := lambda.New(
		session.New(),
		&aws.Config{Region: aws.String(endpoints.UsEast1RegionID)},
	)

	type lambdaInput struct {
		Count   int    `json:"count"`
		Reads   int    `json:"reads"`
		Writes  int    `json:"writes"`
		elb     string `json:"elb"`
	}

	tascInput := lambdaInput{
		Count:   1,
		elb:     elbEndpoint,
		Reads:   numReads,
		Writes:  numWrites,
	}
	payload, _ := json.Marshal(&tascInput)

	input := &lambda.InvokeInput{
		FunctionName:   aws.String("nontasc"),
		Payload:        payload,
		InvocationType: aws.String("RequestResponse"),
	}

	benchStart := time.Now()

	for i := 0; i < numRequests; i++ {
		lambdaStart := time.Now()
		response, err := lambdaClient.Invoke(input)
		lambdaEnd := time.Now()
		latencies = append(latencies, lambdaEnd.Sub(lambdaStart).Seconds())

		if err != nil {
			errors = append(errors, err.Error())
		} else {
			// Next, we try to parse the response.
			bts := string(response.Payload)

			if !strings.Contains(bts, "Success") {
				errors = append(errors, bts)
			}
		}
	}
	benchEnd := time.Now()
	latency <- latencies
	errorsChannel <- errors
	totalTime <- benchEnd.Sub(benchStart).Seconds()
}

/*
* FUNCTIONS FOR LOCAL TESTING
 */
func tascLocal(
	uniqueThread int,
	elbEndpoint string,
	numReq int,
	numWrites int,
	numReads int,
	latency chan []float64,
	errorsChannel chan[]string,
	totalTime chan float64) {
	txnManagerAddr, err := getTASCClientAddr(elbEndpoint)
	if err != nil {
		fmt.Printf("Unexpected error:\n%v\n", err)
		os.Exit(1)
	}
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", txnManagerAddr, cmn.TxnManagerServerPort), grpc.WithInsecure())
	if err != nil {
		fmt.Printf("Unexpected error:\n%v\n", err)
		os.Exit(1)
	}
	client := pb.NewTascClient(conn)

	latencies := make([]float64, 0)
	errors := make([]string, 0)

	benchStart := time.Now()
	writeData := make([]byte, 4096)
	rand.Read(writeData)

	for i := 0; i < numReq; i++ {
		txnStart := time.Now()
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
			_, err := client.Write(context.TODO(), write)
			if err != nil {
				errors = append(errors,
					fmt.Sprintf("Write failed for key %s transaction %d on thread %d\n - %s",
						key, tid, i, err.Error()))
			}
		}
		for j := 0; j < numReads; j++ {
			readKey := rand.Intn(numWrites)
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
			_, err = client.Read(context.TODO(), read)
			if err != nil {
				errors = append(errors,
					fmt.Sprintf("Read failed for key %s transaction %d on thread %d\n - %s",
						key, tid, i, err.Error()))
			}
		}
		resp, err := client.CommitTransaction(context.TODO(), &pb.TransactionTag{
			Tid: tid,
		})
		txnEnd := time.Now()
		if resp.Status != pb.TascTransactionStatus_COMMITTED {
			errors = append(errors,
				fmt.Sprintf("Commit failed for transaction %d on thread %d\n - %s", tid, i, err.Error()))
		}
		latencies = append(latencies, txnEnd.Sub(txnStart).Seconds())
	}
	benchEnd := time.Now()
	latency <- latencies
	errorsChannel <- errors
	totalTime <- benchEnd.Sub(benchStart).Seconds()
}

/*
* HELPER FUNCTIONS FOR BENCHMARKS
 */

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

func printErrors(errors []string, title string) {
	fmt.Println(title)
	for i := 0; i < len(errors); i++ {
		fmt.Println(errors[i])
	}
	fmt.Println()
}

func printThroughput(throughput []float64, numClients int, title string) {
	fmt.Println(title)
	fmt.Printf("Number of Clients: %d", numClients)
	throughputVal, _ := stats.Sum(throughput)
	fmt.Printf("\tTotal Throughput: %.6f\n", throughputVal)
	fmt.Println()
}