#!/usr/bin/env python3

import subprocess
import zmq

def main():
    context = zmq.Context(1)
    benchmark_socket = context.socket(zmq.REP)
    benchmark_socket.bind('tcp://*:6500')

    while True:
        command = benchmark_socket.recv_string()
        splits = command.split(':')
        elb_address = splits[0]
        type_benchmark = splits[1]
        num_threads = int(splits[2])
        num_requests = int(splits[3])
        num_reads = int(splits[4])
        num_writes = int(splits[5])


        cmd = [
            './benchmark',
            '-numThreads', str(num_threads),
            '-numWrites', str(num_writes),
            '-numReads', str(num_reads),
            '-numReq', str(num_requests),
            '-address', elb_address,
            '-type', type_benchmark
        ]
        
        result = subprocess.run(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

        if result.returncode == 0:
            output = str(result.stdout, 'utf-8')
        else:
            output = str(result.stdout, 'utf-8') + '\n' + str(result.stderr, 'utf-8')
        benchmark_socket.send_string(output)

if __name__ == '__main__':
    main()