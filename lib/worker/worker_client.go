package worker

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	cmn "github.com/saurav-c/tasc/lib/common"
	tpb "github.com/saurav-c/tasc/proto/tasc"
	"os"
)

type WorkerClient struct {
	responseIP string
	pusher     *zmq.Socket
}

func NewWorkerClient(ilbAddress string, ipAddress string) *WorkerClient {
	context, err := zmq.NewContext()

	if err != nil {
		fmt.Println("Unexpected error while creating ZeroMQ context. Exiting:\n", err)
		os.Exit(1)
	}

	workerAddr := fmt.Sprintf(cmn.PushTemplate, ilbAddress, cmn.WorkerPullPort)
	socket := cmn.CreateSocket(zmq.PUSH, context, workerAddr, false)

	return &WorkerClient{
		responseIP: ipAddress,
		pusher:     socket,
	}
}

func (wc *WorkerClient) sendWork(tid string, status tpb.TascTransactionStatus, writeset []string) {
	tag := &tpb.TransactionTag{
		Tid:          tid,
		Status:       status,
		TxnManagerIP: wc.responseIP,
	}
	ws := &tpb.TransactionWriteSet{
		Keys: writeset,
	}
	result := &tpb.TransactionResult{
		Tag:      tag,
		Writeset: ws,
	}
	data, _ := proto.Marshal(result)

	wc.pusher.SendBytes(data, zmq.DONTWAIT)
}
