#!/usr/bin/env python3
import zmq
import boto3
import time
import numpy as np

client = boto3.client('lambda')

def main():
    context = zmq.Context(1)
    benchmark_socket = context.socket(zmq.REP)
    benchmark_socket.bind('tcp://*:6500')

    context = zmq.Context(1)
    lambda_socket = context.socket(zmq.REP)
    lambda_socket.bind('tcp://*:6600')

    while True:
        command = benchmark_socket.recv_string()
        splits = command.split(':')
        elb_address = splits[0]
        lambda_name = splits[1]
        num_invokes = int(splits[2])
        num_reads = int(splits[3])
        num_writes = int(splits[4])
        num_txn = int(splits[5])

        
        lambda_payload = """{
            "num_reads": {},
            "num_writes": {},
            "num_txn": {},
            "elb": {}
        }""" % num_reads, num_writes, num_txn, elb_address

        lambda_payload = bytes(lambda_payload)


        start_time = time.time()
        
        for _ in range (num_invokes):
            response = client.invoke(
                FunctionName=lambda_name,
                InvocationType='Event',
                Payload=lambda_payload
            )
        
        latencies = []

        for _ in range(num_invokes):
            benchmark_data = lambda_socket.recv_string()
            benchmark_data = benchmark_data.split(";")
            latency = benchmark_data[0]
            latencies.append(latency)
        
        end_time = time.time()

        throughput = (end_time - start_time)/num_invokes

        latencies = np.array(latencies)
        median = np.percentile(latencies, 50)
        fifth_percent = np.percentile(latencies, 5)
        ninefifth_percent = np.percentile(latencies, 95)
        one_percent = np.percentile(latencies, 1)
        nineone_percent = np.percentile(latencies, 99)

        output = "The throughput of the system is: " + str(throughput) + "\n" + \
                        "The latency histogram is: " + \
                             "Median latency: {}\n5th percentile/95th percentile: {}, {}\n1st percentile/99th percentile: {}, {}\n" % \
                                median, fifth_percent, ninefifth_percent, one_percent, nineone_percent
    
        benchmark_socket.send_string(output)

if __name__ == '__main__':
    main()