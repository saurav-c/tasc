package main

import (
	"fmt"
	"github.com/saurav-c/tasc/lib/storage"
	"os"
)

func main() {
	args := os.Args
	elb := args[1]
	ip := args[2]

	anna := storage.NewAnnaStorageManager(ip, elb)

	val := []byte("test-value")
	err := anna.Put("test", val)
	if err != nil {
		fmt.Printf("PUT Error %s\n", err.Error())
		os.Exit(1)
	}
	v, err := anna.Get("test")
	if err != nil {
		fmt.Printf(" GET Error %s\n", err.Error())
		os.Exit(1)
	}
	s := string(v)
	fmt.Printf("Received value %s\n", s)
}
