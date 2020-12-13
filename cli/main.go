package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	pb "github.com/saurav-c/aftsi/proto/aftsi"

	zmq "github.com/pebbe/zmq4"
	"google.golang.org/grpc"
)

func main() {
	fmt.Println("TASC Command Line Interface")
	if len(os.Args) == 1 {
		fmt.Println("Please pass in the address of the TASC Transaction Router.")
		return
	}

	elbEndpoint := os.Args[1]
	zctx, err := zmq.NewContext()
	if err != nil {
		fmt.Printf("An error %s has occurred.\n", err)
		return
	}

	sckt, err := zctx.NewSocket(zmq.REQ)
	err = sckt.Connect(fmt.Sprintf("tcp://%s:8000", elbEndpoint))
	if err != nil {
		fmt.Printf("An error %s has occurred.\n", err)
		return
	}

	reader := bufio.NewReader(os.Stdin)
	tidClientMapping := map[string]pb.AftSIClient{}

	for {
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')
		splitStringInput := strings.Split(text, " ")
		command := splitStringInput[0]
		command = strings.TrimSpace(command)
		switch command {
		case "start":
			sckt.SendBytes(nil, zmq.DONTWAIT)
			txnAddressBytes, _ := sckt.RecvBytes(0)
			txnAddress := string(txnAddressBytes)
			fmt.Println(txnAddress)
			conn, err := grpc.Dial(fmt.Sprintf("%s:5000", txnAddress), grpc.WithInsecure())
			tascClient := pb.NewAftSIClient(conn)
			start := time.Now()
			tid, err := tascClient.StartTransaction(context.TODO(), &empty.Empty{})
			end := time.Now()
			tidClientMapping[tid.Tid] = tascClient
			fmt.Printf("Start took: %f ms\n", 1000*end.Sub(start).Seconds())
			if err != nil {
				fmt.Printf("An error %s has occurred.\n", err)
				return
			}
			fmt.Printf("The tid we are using is: %s\n", tid.GetTid())
		case "read":
			if len(splitStringInput) != 3 {
				fmt.Println("Incorrect usage: read <TID> <key>")
				continue
			}
			tid := strings.TrimSpace(splitStringInput[1])
			keyToFetch := strings.TrimSpace(splitStringInput[2])
			readReq := &pb.ReadRequest{
				Tid: tid,
				Key: keyToFetch,
			}
			tascClient := tidClientMapping[tid]
			start := time.Now()
			response, err := tascClient.Read(context.TODO(), readReq)
			end := time.Now()
			fmt.Printf("Read took: %f ms\n", 1000*end.Sub(start).Seconds())
			if err != nil {
				fmt.Printf("An error %s has occurred.\n", err)
				return
			}
			fmt.Printf("The value received is: %s\n", string(response.Value))
		case "write":
			if len(splitStringInput) != 4 {
				fmt.Println("Incorrect usage: write <TID> <key> <value>")
				continue
			}
			tid := strings.TrimSpace(splitStringInput[1])
			keyToWrite := strings.TrimSpace(splitStringInput[2])
			valueToWrite := strings.TrimSpace(splitStringInput[3])
			writeReq := &pb.WriteRequest{
				Tid:   tid,
				Key:   keyToWrite,
				Value: []byte(valueToWrite),
			}
			tascClient := tidClientMapping[tid]
			start := time.Now()
			_, err := tascClient.Write(context.TODO(), writeReq)
			end := time.Now()
			fmt.Printf("Write took: %f ms\n", 1000*end.Sub(start).Seconds())
			if err != nil {
				fmt.Printf("An error %s has occurred.\n", err)
				return
			}
			fmt.Println("The write was successful.")
		case "commit":
			if len(splitStringInput) != 2 {
				fmt.Println("Incorrect usage: commit <TID>")
				continue
			}
			tid := strings.TrimSpace(splitStringInput[1])
			TID := &pb.TransactionID{
				Tid: tid,
				E:   0,
			}
			tascClient := tidClientMapping[tid]
			start := time.Now()
			resp, err := tascClient.CommitTransaction(context.TODO(), TID)
			end := time.Now()
			fmt.Printf("Commit took: %f ms\n", 1000*end.Sub(start).Seconds())
			if err != nil {
				fmt.Printf("An error %s has occurred.\n", err)
				return
			}
			if resp.GetE() == pb.TransactionError_FAILURE {
				fmt.Println("Transaction ABORTED")
				continue
			}

			fmt.Println("The commit was successful.")
		case "abort":
			if len(splitStringInput) != 2 {
				fmt.Println("Incorrect usage: abort <TID>")
				continue
			}
			tid := strings.TrimSpace(splitStringInput[1])
			TID := &pb.TransactionID{
				Tid: tid,
				E:   0,
			}
			tascClient := tidClientMapping[tid]
			_, err := tascClient.AbortTransaction(context.TODO(), TID)
			if err != nil {
				fmt.Printf("An error %s has occurred.\n", err)
				return
			}
			fmt.Println("The abort was successful.")
		default:
			fmt.Println("Not a valid command: " + command)
		}
		fmt.Println("")
	}
}
