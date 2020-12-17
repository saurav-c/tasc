package common

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	mpb "github.com/saurav-c/aftsi/proto/monitor"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type StatsMonitor struct {
	pusher   *zmq.Socket
	stats    map[string]*mpb.LatencyList
	lock     *sync.Mutex
	addr     string
	nodeType mpb.NodeType
}

func NewStatsMonitor(node mpb.NodeType, nodeAddr string, monitorAddr string) (*StatsMonitor, error) {
	zctx, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}
	statsPusher := CreateSocket(zmq.PUSH, zctx, fmt.Sprintf(PushTemplate, monitorAddr, MonitorPushPort), false)
	return &StatsMonitor{
		pusher:   statsPusher,
		stats:    make(map[string]*mpb.LatencyList),
		lock:     &sync.Mutex{},
		addr:     nodeAddr,
		nodeType: node,
	}, nil
}

func (monitor *StatsMonitor) TrackFuncExecTime(tid string, msg string, start time.Time) {
	diff := time.Now().Sub(start)
	go monitor.TrackStat(tid, msg, diff)
}

func (monitor *StatsMonitor) TrackStat(tid string, msg string, diff time.Duration) {
	latency := diff.Seconds() * 1000
	log.Debugf("%s for Txn %s: %f ms", msg, tid, latency)
	monitor.lock.Lock()
	defer monitor.lock.Unlock()
	if _, ok := monitor.stats[msg]; !ok {
		monitor.stats[msg] = &mpb.LatencyList{}
	}
	lat := &mpb.Latency{
		Tid:   tid,
		Value: latency,
	}
	monitor.stats[msg].Latencies = append(monitor.stats[msg].Latencies, lat)
}

func (monitor *StatsMonitor) SendStats(frequency time.Duration) {
	for true {
		time.Sleep(frequency)
		monitor.lock.Lock()
		if len(monitor.stats) == 0 {
			monitor.lock.Unlock()
			continue
		}
		statsMsg := &mpb.Statistics{
			Addr:  monitor.addr,
			Node:  monitor.nodeType,
			Stats: monitor.stats,
		}
		monitor.stats = make(map[string]*mpb.LatencyList)
		monitor.lock.Unlock()
		data, _ := proto.Marshal(statsMsg)
		monitor.pusher.SendBytes(data, zmq.DONTWAIT)
	}
}
