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
        num_txn = int(splits[2])
        num_reads = int(splits[3])
        num_writes = int(splits[4])
        ip_addr = splits[5]

        lambda_payload = """{
            "num_reads": {},
            "num_writes": {},
            "num_txns": {},
            "elb": {},
            "benchmark_ip": {}
        }""" % num_reads, num_writes, num_txn, elb_address, ip_addr

        lambda_payload = bytes(lambda_payload)
        error_lambda = 0

        for _ in range (num_txn):
            response = client.invoke(
                FunctionName=lambda_name,
                InvocationType='Event',
                Payload=lambda_payload
            )
            if response["StatusCode"] > 299:
                error_lambda += 1
        
        num_invokes = num_invokes - error_lambda

        throughputs = []
        latencies = []
        start_txn = []
        write_txn = []
        read_txn = []
        commit_txn = []
        ip_resolt = []

        for _ in range(num_invokes):
            benchmark_data = lambda_socket.recv_string()
            benchmark_data = benchmark_data.split(";")
            throughput = float(benchmark_data[0])
            latency = [float(x) for x in benchmark_data[1].split(",")]
            ip_resolt_time = [float(x) for x in benchmark_data[2].split(",")]
            start_txn_time = [float(x) for x in benchmark_data[3].split(",")]
            write_txn_time = [float(x) for x in benchmark_data[4].split(",")]
            read_txn_time = [float(x) for x in benchmark_data[5].split(",")]
            commit_txn_time = [float(x) for x in benchmark_data[6].split(",")]
            throughputs.append(throughput)
            latencies.append(*latency)
            ip_resolt.append(*ip_resolt_time)
            start_txn.append(*start_txn_time)
            write_txn.append(*write_txn_time)
            read_txn.append(*read_txn_time)
            commit_txn.append(*commit_txn_time)

        throughput = sum(throughputs)

        latencies = np.array(latencies)
        median_latency = np.percentile(latencies, 50)
        fifth_latency = np.percentile(latencies, 5)
        ninefifth_latency = np.percentile(latencies, 95)
        one_latency = np.percentile(latencies, 1)
        nineone_latency = np.percentile(latencies, 99)
        
        start_txn = np.array(start_txn)
        median_start = np.percentile(start_txn, 50)
        fifth_start = np.percentile(start_txn, 5)
        ninefifth_start = np.percentile(start_txn, 95)
        one_start = np.percentile(start_txn, 1)
        nineone_start = np.percentile(start_txn, 99)

        write_txn = np.array(write_txn)
        median_write = np.percentile(write_txn, 50)
        fifth_write = np.percentile(write_txn, 5)
        ninefifth_write = np.percentile(write_txn, 95)
        one_write = np.percentile(write_txn, 1)
        nineone_write = np.percentile(write_txn, 99)

        read_txn = np.array(read_txn)
        median_read = np.percentile(read_txn, 50)
        fifth_read = np.percentile(read_txn, 5)
        ninefifth_read = np.percentile(read_txn, 95)
        one_read = np.percentile(read_txn, 1)
        nineone_read = np.percentile(read_txn, 99)

        commit_txn = np.array(commit_txn)
        median_commit = np.percentile(commit_txn, 50)
        fifth_commit = np.percentile(commit_txn, 5)
        ninefifth_commit = np.percentile(commit_txn, 95)
        one_commit = np.percentile(commit_txn, 1)
        nineone_commit = np.percentile(commit_txn, 99)

        ip_resolt = np.array(ip_resolt)
        median_ip_resolt = np.percentile(ip_resolt, 50)
        fifth_ip_resolt = np.percentile(ip_resolt, 5)
        ninefifth_ip_resolt = np.percentile(ip_resolt, 95)
        one_ip_resolt = np.percentile(ip_resolt, 1)
        nineone_ip_resolt = np.percentile(ip_resolt, 99)

        output = "A total of {} lambda functions ran.\n" % {num_invokes}
        output += "The throughput of the system is: " + str(throughput) + "\n" + \
                        "The latency histogram is: " + \
                             "Median latency: {}\n5th percentile/95th percentile: {}, {}\n1st percentile/99th percentile: {}, {}\n" % \
                                median_latency, fifth_latency, ninefifth_latency, one_latency, nineone_latency
        output += "The IP resolution start txn histogram is: " + \
                        "Median latency: {}\n5th percentile/95th percentile: {}, {}\n1st percentile/99th percentile: {}, {}\n" % \
                            median_ip_resolt, fifth_ip_resolt, ninefifth_ip_resolt, one_ip_resolt, nineone_ip_resolt
        output += "The start txn histogram is: " + \
                        "Median latency: {}\n5th percentile/95th percentile: {}, {}\n1st percentile/99th percentile: {}, {}\n" % \
                            median_start, fifth_start, ninefifth_start, one_start, nineone_start
        output += "The write txn histogram is: " + \
                        "Median latency: {}\n5th percentile/95th percentile: {}, {}\n1st percentile/99th percentile: {}, {}\n" % \
                            median_write, fifth_write, ninefifth_write, one_write, nineone_write
        output += "The read txn histogram is: " + \
                        "Median latency: {}\n5th percentile/95th percentile: {}, {}\n1st percentile/99th percentile: {}, {}\n" % \
                            median_read, fifth_read, ninefifth_read, one_read, nineone_read
        output += "The commit txn histogram is: " + \
                        "Median latency: {}\n5th percentile/95th percentile: {}, {}\n1st percentile/99th percentile: {}, {}\n" % \
                            median_commit, fifth_commit, ninefifth_commit, one_commit, nineone_commit

        benchmark_socket.send_string(output)

if __name__ == '__main__':
    main()