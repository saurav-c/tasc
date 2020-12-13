#!/usr/bin/env python3

import argparse
import datetime
import sys

import zmq

def main():
    parser = argparse.ArgumentParser(description='Starts an Aft benchmark.')
    parser.add_argument('-r', '--numRequests', nargs=1, type=int, metavar='R',
                        help='The number of requests to run per thread.',
                        dest='requests', required=True)
    parser.add_argument('-t', '--numThreads', nargs=1, type=int, metavar='T',
                        help='The number of threads per server to run.',
                        dest='threads', required=True)
    parser.add_argument('-a', '--address', nargs=1, type=str, metavar='A',
                        help='ELB Address for the Load Balancer Values', 
                        dest='address', required=True)
    parser.add_argument('-y', '--type', nargs=1, type=str, metavar='Y',
                        help='Type of Benchmark to Run', dest='type',
                        required=True)

    args = parser.parse_args()

    servers = []
    with open('benchmarks.txt') as f:
        lines = f.readlines()
        for line in lines:
            servers.append(line.strip())

    print('Found %d servers:%s' % (len(servers), '\n\t-' + '\n\t-'.join(servers)))

    message = ('%d:%d:%s:%s') % (args.threads[0], args.threads[0] *
                              args.requests[0], args.address[0], args.type[0])

    conns = []
    context = zmq.Context(1)
    print('Starting benchmark at %s' % (str(datetime.datetime.now())))
    for server in servers:
        conn = context.socket(zmq.REQ)
        address = ('tcp://%s:6500') % server
        conn.connect(address)
        conn.send_string(message)

        conns.append(conn)

    for conn in conns:
        response = conn.recv_string()
        print(str(response))

    print('Finished!')

if __name__ == '__main__':
    main()