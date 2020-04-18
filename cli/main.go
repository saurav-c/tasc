package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

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
	globalTid := ""

	for {
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')
		splitStringInput := strings.Split(text, " ")
		command := splitStringInput[0]
		switch command{
		case "start":
			tid, err := client.StartTransaction(context.TODO(), &empty.Empty{})
			if err != nil {
				fmt.Printf("An error %s has occurred.\n", err)
				return
			}
			globalTid = tid.GetTid()
			fmt.Printf("The tid we are using is: %s\n", tid.GetTid())
		case "read":
			keyToFetch := splitStringInput[1]
			readReq := &pb.ReadRequest{
				Tid: globalTid,
				Key: keyToFetch,
			}
			response, err := client.Read(context.TODO(), readReq)
			if err != nil {
				fmt.Printf("An error %s has occurred.\n", err)
				return
			}
			fmt.Printf("The value received is: %s.\n", response.Value)
		case "write":
			keyToWrite := splitStringInput[1]
			valueToWrite := splitStringInput[2]
			writeReq := &pb.WriteRequest{
				Tid:   globalTid,
				Key:   keyToWrite,
				Value: []byte(valueToWrite),
			}
			_, err := client.Write(context.TODO(), writeReq)
			if err != nil {
				fmt.Printf("An error %s has occurred.\n", err)
				return
			}
			fmt.Println("The write was successful.")
		case "commit":
			tid := &pb.TransactionID{
				Tid: globalTid,
				E:   0,
			}
			_, err := client.CommitTransaction(context.TODO(), tid)
			if err != nil {
				fmt.Printf("An error %s has occurred.\n", err)
				return
			}
			fmt.Println("The commit was successful.")
		case "abort":
			tid := &pb.TransactionID{
				Tid: globalTid,
				E:   0,
			}
			_, err := client.AbortTransaction(context.TODO(), tid)
			if err != nil {
				fmt.Printf("An error %s has occurred.\n", err)
				return
			}
			fmt.Println("The abort was successful.")
		}
		fmt.Println("")
	}
}
