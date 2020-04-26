package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	pb "github.com/saurav-c/aftsi/proto/aftsi/api"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

func main() {
	fmt.Println("AFTSI Command Line Interface")
	if len(os.Args) == 1 {
		fmt.Println("Please pass in the address of the AFTSI replica.")
		return
	}
	address := os.Args[1]
	conn, err := grpc.Dial(fmt.Sprintf("%s:5000", address), grpc.WithInsecure())
	defer conn.Close()
	if err != nil {
		fmt.Printf("Unexpected error:\n%v\n", err)
		os.Exit(1)
	}
	client := pb.NewAftSIClient(conn)
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')
		splitStringInput := strings.Split(text, " ")
		command := splitStringInput[0]
		command = strings.TrimSpace(command)
		switch command {
		case "start":
			start := time.Now()
			tid, err := client.StartTransaction(context.TODO(), &empty.Empty{})
			end := time.Now()
			fmt.Printf("Start took: %f ms\n", end.Sub(start).Seconds())
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
			start := time.Now()
			response, err := client.Read(context.TODO(), readReq)
			end := time.Now()
			fmt.Printf("Read took: %f ms\n", end.Sub(start).Seconds())
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
			start := time.Now()
			_, err := client.Write(context.TODO(), writeReq)
			end := time.Now()
			fmt.Printf("Write took: %f ms\n", end.Sub(start).Seconds())
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
			start := time.Now()
			resp, err := client.CommitTransaction(context.TODO(), TID)
			end := time.Now()
			fmt.Printf("Commit took: %f ms\n", end.Sub(start).Seconds())
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
			_, err := client.AbortTransaction(context.TODO(), TID)
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
