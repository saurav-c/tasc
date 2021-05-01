#!/usr/bin/env python3
import zmq
import boto3
import numpy as np
import os
import yaml
import json

client = boto3.client('lambda')


def main():
    context = zmq.Context()
    benchmark_socket = context.socket(zmq.REP)
    benchmark_socket.bind('tcp://*:6500')

    lambda_socket = context.socket(zmq.PULL)
    lambda_socket.bind('tcp://*:6600')

    # Get this server's IP
    server_ip = None
    filename = os.environ['TASC_HOME'] + '/config/tasc-config.yml'
    with open(filename, 'r') as f:
        d = yaml.safe_load(f.read())
        server_ip = d['publicIP']

    while True:
        data = benchmark_socket.recv_string()
        config = json.loads(data)

        lambda_name = config['lambda']
        num_clients = config['num_clients']
        elb_address = config['elb']
        num_txns = config['num_txns']
        num_reads = config['num_reads']
        num_writes = config['num_writes']
        zipf = config['zipf']
        prefix = config['prefix']
        N = config['N']

        payload = {
            'num_txns': num_txns,
            'num_reads': num_reads,
            'num_writes': num_writes,
            'elb': elb_address,
            'benchmark_ip': server_ip,
            'zipf': zipf,
            'prefix': prefix,
            'N': N
        }
        lambda_payload = json.dumps(payload)

        num_invokes, error_lambda = 0, 0
        for _ in range(num_clients):
            response = client.invoke(
                FunctionName=lambda_name,
                InvocationType='Event',
                Payload=lambda_payload
            )
            if response["StatusCode"] > 299:
                error_lambda += 1
            else:
                num_invokes += 1

        throughputs = []
        latencies = []
        lb_txn = []
        start_txn = []
        write_txn = []
        read_txn = []
        commit_txn = []
        num_aborts = 0.0
        num_failed_reads = 0.0

        for _ in range(num_invokes):
            benchmark_data = lambda_socket.recv_string()
            benchmark_data = benchmark_data.split(";")

            throughput = float(benchmark_data[0])
            latency = [float(x) for x in benchmark_data[1].split(",")]
            lb_txn_time = [float(x) for x in benchmark_data[2].split(",")]
            start_txn_time = [float(x) for x in benchmark_data[3].split(",")]
            write_txn_time = [float(x) for x in benchmark_data[4].split(",")]
            read_txn_time = [float(x) for x in benchmark_data[5].split(",")]
            commit_txn_time = [float(x) for x in benchmark_data[6].split(",")]

            num_aborts += float(benchmark_data[7])
            num_failed_reads += float(benchmark_data[8])

            throughputs.append(throughput)
            latencies.extend(latency)
            lb_txn.extend(lb_txn_time)
            start_txn.extend(start_txn_time)
            write_txn.extend(write_txn_time)
            read_txn.extend(read_txn_time)
            commit_txn.extend(commit_txn_time)

        throughput = sum(throughputs)

        latencies = np.array(latencies)
        median_latency = np.percentile(latencies, 50)
        fifth_latency = np.percentile(latencies, 5)
        ninety_fifth_latency = np.percentile(latencies, 95)
        one_latency = np.percentile(latencies, 1)
        ninety_ninth_latency = np.percentile(latencies, 99)

        start_txn = np.array(start_txn)
        median_start = np.percentile(start_txn, 50)
        fifth_start = np.percentile(start_txn, 5)
        ninety_fifth_start = np.percentile(start_txn, 95)
        one_start = np.percentile(start_txn, 1)
        ninety_ninth_start = np.percentile(start_txn, 99)

        write_txn = np.array(write_txn)
        median_write = np.percentile(write_txn, 50)
        fifth_write = np.percentile(write_txn, 5)
        ninety_fifth_write = np.percentile(write_txn, 95)
        one_write = np.percentile(write_txn, 1)
        ninety_ninth_write = np.percentile(write_txn, 99)

        read_txn = np.array(read_txn)
        median_read = np.percentile(read_txn, 50)
        fifth_read = np.percentile(read_txn, 5)
        ninety_fifth_read = np.percentile(read_txn, 95)
        one_read = np.percentile(read_txn, 1)
        ninety_ninth_read = np.percentile(read_txn, 99)

        commit_txn = np.array(commit_txn)
        median_commit = np.percentile(commit_txn, 50)
        fifth_commit = np.percentile(commit_txn, 5)
        ninety_fifth_commit = np.percentile(commit_txn, 95)
        one_commit = np.percentile(commit_txn, 1)
        ninety_ninth_commit = np.percentile(commit_txn, 99)

        lb_txn = np.array(lb_txn)
        median_lb = np.percentile(lb_txn, 50)
        fifth_lb = np.percentile(lb_txn, 5)
        ninety_fifth_lb = np.percentile(lb_txn, 95)
        one_lb = np.percentile(lb_txn, 1)
        ninety_ninth_lb = np.percentile(lb_txn, 99)

        abort_ratio = num_aborts / (num_txns * num_clients)
        failed_reads_ratio = num_failed_reads / (num_txns * num_reads) if num_reads > 0 else 0.0

        output = "A total of {} lambda functions ran.\n".format(num_invokes)
        output += "The throughput of the system is: " + str(throughput) + "\n" + \
                  "The latency (w/o load balancing) histogram is: " + \
                  "Median latency: {}\n5th percentile/95th percentile: {}, {}\n1st percentile/99th percentile: {}, {}\n" \
                  .format(median_latency, fifth_latency, ninety_fifth_latency, one_latency, ninety_ninth_latency)
        output += "The load balancing txn histogram is: " + \
                  "Median latency: {}\n5th percentile/95th percentile: {}, {}\n1st percentile/99th percentile: {}, {}\n" \
                  .format(median_lb, fifth_lb, ninety_fifth_lb, one_lb, ninety_ninth_lb)
        output += "The start txn histogram is: " + \
                  "Median latency: {}\n5th percentile/95th percentile: {}, {}\n1st percentile/99th percentile: {}, {}\n" \
                  .format(median_start, fifth_start, ninety_fifth_start, one_start, ninety_ninth_start)
        output += "The write txn histogram is: " + \
                  "Median latency: {}\n5th percentile/95th percentile: {}, {}\n1st percentile/99th percentile: {}, {}\n" \
                  .format(median_write, fifth_write, ninety_fifth_write, one_write, ninety_ninth_write)
        output += "The read txn histogram is: " + \
                  "Median latency: {}\n5th percentile/95th percentile: {}, {}\n1st percentile/99th percentile: {}, {}\n" \
                  .format(median_read, fifth_read, ninety_fifth_read, one_read, ninety_ninth_read)
        output += "The commit txn histogram is: " + \
                  "Median latency: {}\n5th percentile/95th percentile: {}, {}\n1st percentile/99th percentile: {}, {}\n" \
                  .format(median_commit, fifth_commit, ninety_fifth_commit, one_commit, ninety_ninth_commit)
        output += "Abort Ratio is: {}\n".format(abort_ratio)
        output += "Failed Reads Ratio is: {}\n".format(failed_reads_ratio)

        # Send stats back to trigger
        benchmark_socket.send_string(output)


if __name__ == '__main__':
    main()
