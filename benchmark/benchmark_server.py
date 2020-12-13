#!/usr/bin/env python3

import os
import subprocess

import zmq

def main():
    context = zmq.Context(1)
    benchmark_socket = context.socket(zmq.REP)
    port = 6500

    benchmark_socket.bind('tcp://*:%d' % port)

    while True:
        command = benchmark_socket.recv_string()
        splits = command.split(':')
        num_threads = int(splits[0])
        num_requests = int(splits[1])
        elb_address = splits[2]
        type_benchmark = splits[3]

        cmd = [
            './benchmark',
            '-numThreads', str(num_threads),
            '-numReq', str(num_requests),
            '-address', elb_address,
            '-type', type_benchmark
        ]
        
        result = subprocess.run(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

        if result.returncode == 0:
            output = str(result.stdout, 'utf-8')
        else:
            output = str(result.stdout, 'utf-8') + '\n' + str(result.stderr,
                                                              'utf-8')

        benchmark_socket.send_string(output)

if __name__ == '__main__':
    main()