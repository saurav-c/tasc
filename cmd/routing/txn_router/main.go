package main

import (
	"crypto/sha1"
	pb "github.com/saurav-c/aftsi/proto/routing/api"
	"log"
)

func _convertByteToInt(arr []byte) (int) {
	retVal := 0
	for _, elem := range arr {
		retVal = retVal + int(elem)
	}
	return retVal
}

func (t *TxnRouter) lookup(req pb.KeyRouterReq) (*pb.RouterResponse, error){
	keyLookup := req.GetKey()
	h := sha1.New()
	keySha := h.Sum([]byte(keyLookup))
	index := _convertByteToInt(keySha) % len(t.router)
	ipAddress := t.router[index]
	return &pb.RouterResponse{
		Ip:    ipAddress,
		Error: 0,
	}, nil
}

func main() {
	ip := ""
	keyNode, err := NewKeyNode(ip, "local")
	if err != nil {
		log.Fatalf("Could not start new Key Node %v\n", err)
	}

	startKeyNode(keyNode)
}