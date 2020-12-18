#!/usr/bin/env python3

import argparse
import datetime
import zmq

def main():
    parser = argparse.ArgumentParser(description='Makes a call to the TASC benchmark server.')
    parser.add_argument('-r', '--numRequests', nargs=1, type=int, metavar='R',
                        help='The total number of requests to run.',
                        dest='requests', required=True)
    parser.add_argument('-t', '--numThreads', nargs=1, type=int, metavar='T',
                        help='The number of threads per server to run.',
                        dest='threads', required=True)
    parser.add_argument('-a', '--address', nargs=1, type=str, metavar='A',
                        help='ELB Address for the Load Balancer Values.', 
                        dest='address', required=True)
    parser.add_argument('-y', '--type', nargs=1, type=str, metavar='Y',
                        help='The type of benchmark to be run on the server.', 
                        dest='type', required=True)
    args = parser.parse_args()

    servers = []
    with open('benchmarks.txt') as f:
        lines = f.readlines()
        for line in lines:
            servers.append(line.strip())

    print('Found %d servers: \n%s' % (len(servers), ' '.join(servers)))

    message = ('%d:%d:%s:%s') % (args.threads[0], args.requests[0], args.address[0], args.type[0])

    conns = []
    context = zmq.Context(1)
    print('Starting benchmark at %s' % (str(datetime.datetime.now())))
    for server in servers:
        conn = context.socket(zmq.REQ)
        address = ('tcp://%s:6500') % server
        conn.connect(address)
        conn.send_string(message)
        conns.append(conn)

    print('Printing output from benchmark runs...')
    for conn in conns:
        response = conn.recv_string()
        print(str(response))

    print('Finished!')

if __name__ == '__main__':
    main()