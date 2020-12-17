package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	cmn "github.com/saurav-c/tasc/lib/common"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	pb "github.com/saurav-c/tasc/proto/tasc"

	zmq "github.com/pebbe/zmq4"
	"google.golang.org/grpc"
)

func main() {
	local := flag.Bool("local", false, "Local Mode")
	flag.Parse()

	zctx, err := zmq.NewContext()
	if err != nil {
		fmt.Printf("An error %s has occurred.\n", err)
		return
	}

	sckt, err := zctx.NewSocket(zmq.REQ)
	defer sckt.Close()

	fmt.Println("TASC Command Line Interface")

	if !(*local) {
		if len(os.Args) == 1 {
			fmt.Println("Please pass in the address of the TASC Transaction Router.")
			return
		}

		elbEndpoint := os.Args[1]
		err = sckt.Connect(fmt.Sprintf("tcp://%s:8000", elbEndpoint))
		if err != nil {
			fmt.Printf("An error %s has occurred.\n", err)
			return
		}
	}

	reader := bufio.NewReader(os.Stdin)
	tidClientMapping := map[string]pb.TascClient{}
	tidMap := map[string]string{}
	tidCounter := 0

	for {
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')
		splitStringInput := strings.Split(text, " ")
		command := splitStringInput[0]
		command = strings.TrimSpace(command)
		switch command {
		case "start":
			txnAddress := "127.0.0.1"
			if !(*local) {
				sckt.SendBytes(nil, zmq.DONTWAIT)
				txnAddressBytes, _ := sckt.RecvBytes(0)
				txnAddress = string(txnAddressBytes)
			}
			fmt.Println(txnAddress)
			conn, err := grpc.Dial(fmt.Sprintf("%s:%d", txnAddress, cmn.TxnManagerServerPort), grpc.WithInsecure())
			tascClient := pb.NewTascClient(conn)
			start := time.Now()
			tid, err := tascClient.StartTransaction(context.TODO(), &empty.Empty{})
			end := time.Now()
			tidClientMapping[tid.Tid] = tascClient
			fmt.Printf("Start took: %f ms\n", 1000*end.Sub(start).Seconds())
			if err != nil {
				fmt.Printf("An error %s has occurred.\n", err)
				return
			}
			aliasTid := strconv.Itoa(tidCounter)
			tidCounter++
			tidMap[aliasTid] = tid.Tid
			fmt.Printf("The tid we are using is: %s\n", tid.GetTid())
			fmt.Printf("You can use a tid alias: %s\n", aliasTid)
		case "read":
			if len(splitStringInput) != 3 {
				fmt.Println("Incorrect usage: read <TID> <key>")
				continue
			}
			tid := strings.TrimSpace(splitStringInput[1])
			tid, ok := tidMap[tid]
			if !ok {
				fmt.Printf("Alias tid %s not found\n", tid)
				continue
			}
			keyToFetch := strings.TrimSpace(splitStringInput[2])
			var keyPairs []*pb.TascRequest_KeyPair
			keyPairs = append(keyPairs, &pb.TascRequest_KeyPair{Key: keyToFetch})
			readReq := &pb.TascRequest{
				Tid:   tid,
				Pairs: keyPairs,
			}
			tascClient, ok := tidClientMapping[tid]
			if !ok {
				fmt.Printf("Unknown tid %s", tid)
				continue
			}
			start := time.Now()
			response, err := tascClient.Read(context.TODO(), readReq)
			end := time.Now()
			fmt.Printf("Read took: %f ms\n", 1000*end.Sub(start).Seconds())
			if err != nil {
				fmt.Printf("An error %s has occurred.\n", err)
				return
			}
			fmt.Printf("The value received is: %s\n", string(response.Pairs[0].Value))
		case "write":
			if len(splitStringInput) != 4 {
				fmt.Println("Incorrect usage: write <TID> <key> <value>")
				continue
			}
			tid := strings.TrimSpace(splitStringInput[1])
			tid, ok := tidMap[tid]
			if !ok {
				fmt.Printf("Alias tid %s not found\n", tid)
				continue
			}
			keyToWrite := strings.TrimSpace(splitStringInput[2])
			valueToWrite := []byte(strings.TrimSpace(splitStringInput[3]))
			var keyPairs []*pb.TascRequest_KeyPair
			keyPairs = append(keyPairs, &pb.TascRequest_KeyPair{Key: keyToWrite, Value: valueToWrite})
			writeReq := &pb.TascRequest{
				Tid:   tid,
				Pairs: keyPairs,
			}
			tascClient, ok := tidClientMapping[tid]
			if !ok {
				fmt.Printf("Unknown tid %s", tid)
				continue
			}
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
			tid, ok := tidMap[tid]
			if !ok {
				fmt.Printf("Alias tid %s not found\n", tid)
				continue
			}
			TID := &pb.TransactionTag{Tid:tid}
			tascClient, ok := tidClientMapping[tid]
			if !ok {
				fmt.Printf("Unknown tid %s", tid)
				continue
			}
			start := time.Now()
			resp, err := tascClient.CommitTransaction(context.TODO(), TID)
			end := time.Now()
			fmt.Printf("Commit took: %f ms\n", 1000*end.Sub(start).Seconds())
			if err != nil {
				fmt.Printf("An error %s has occurred.\n", err)
				return
			}
			if resp.Status != pb.TascTransactionStatus_COMMITTED {
				fmt.Println("ABORTED")
			} else {
				fmt.Println("Successfully COMMITTED")
			}
		case "abort":
			if len(splitStringInput) != 2 {
				fmt.Println("Incorrect usage: abort <TID>")
				continue
			}
			tid := strings.TrimSpace(splitStringInput[1])
			tid, ok := tidMap[tid]
			if !ok {
				fmt.Printf("Alias tid %s not found\n", tid)
				continue
			}
			TID := &pb.TransactionTag{Tid:tid}
			tascClient, ok := tidClientMapping[tid]
			if !ok {
				fmt.Printf("Unknown tid %s", tid)
				continue
			}
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
