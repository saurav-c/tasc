import json
import os
import random

import boto3
import grpc
from google.protobuf import empty_pb2
import numpy as np
import scipy.stats as stats
import zmq
import time

from aftsi_pb2 import *
from aftsi_pb2_grpc import *

lmbd = boto3.client('lambda')

N = 99999
x = np.arange(1, N)
a = 1.5
weights = x ** (-a)
weights /= weights.sum()
bounded_zipf = stats.rv_discrete(name='bounded_zipf', values=(x, weights))

sys_random = random.SystemRandom()

def lambda_handler(event, context):
    num_reads = int(event["read"])
    num_writes = int(event["write"])
    num_transactions = int(event["transactions"])
    throughput = 0

    for i in range(num_transactions):
        print('*** Starting Transaction ' + str(i) + '! ***')
        lb_addr = event["elb"]
        ctx = zmq.Context(1)
        sckt = ctx.socket(zmq.REQ)
        sckt.connect('tcp://%s:8000' % lb_addr)
        sckt.send_string('')
        address = sckt.recv_string()

        num_keys = num_writes

        with grpc.insecure_channel(address + ':5000') as channel:
            client = AftSIStub(channel)
            start_time = time.time()
            txn = client.StartTransaction(empty_pb2.Empty())
            txn_id_str = txn.tid

            keys = []
            for _ in range(num_keys):
                key = str(bounded_zipf.rvs(size=1)[0])
                keys.append(str(key))

            # Do the writes.
            for i in range(num_writes):
                update = WriteRequest(tid=txn_id_str, key=keys[i], value=os.urandom(4096))
                client.Write(update)

            # Do the reads.
            for i in range(num_reads):
                update = ReadRequest(tid=txn_id_str, key=keys[i])
                client.Read(update)

            client.CommitTransaction(txn)
            end_time = time.time()
            throughput += (end_time - start_time)
            print("Latency for transaction " + str(i) + ": " + str((end_time - start_time) * 1000))
    print(str(num_transactions) + " Transactions were run. Throughput (txn/sec): "  + str(num_transactions/throughput))
    return "Success"

