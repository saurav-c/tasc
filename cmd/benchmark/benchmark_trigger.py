#!/usr/bin/env python3

import argparse
import datetime
import zmq

def main():
    parser = argparse.ArgumentParser(description='Makes a call to the TASC benchmark server.')
    parser.add_argument('-a', '--address', nargs=1, type=str, metavar='A',
                        help='ELB Address for the Load Balancer Values.', 
                        dest='address', required=True)
    parser.add_argument('-w', '--writes', nargs=1, type=int, metavar='Y',
                        help='The number of writes to be done.',
                        dest='writes', required=True)
    parser.add_argument('-r', '--reads', nargs=1, type=int, metavar='Y',
                        help='The number of reads to be done.',
                        dest='reads', required=True)
    parser.add_argument('-t', '--txn', nargs=1, type=int, metavar='Y',
                        help='The number of txns to be done.',
                        dest='txn', required=True)
    parser.add_argument('-y', '--type', nargs=1, type=str, metavar='Y',
                        help='The type of lambda to be run.', 
                        dest='type', required=True)
    args = parser.parse_args()

    servers = []
    with open('benchmarks.txt') as f:
        lines = f.readlines()
        for line in lines:
            servers.append(line.strip())

    print('Found %d servers: \n%s' % (len(servers), ' '.join(servers)))

    conns = []
    context = zmq.Context(1)
    print('Starting benchmark at %s' % (str(datetime.datetime.now())))
    for server in servers:
        conn = context.socket(zmq.REQ)
        address = ('tcp://%s:6500') % server
        conn.connect(address)
        message = ('%s:%s:%d:%d:%d:%d:%s') % (args.address[0], args.type[0], args.txn[0], args.reads[0], args.writes[0], server)
        conn.send_string(message)
        conns.append(conn)

    print('Printing output from benchmark runs...')
    for conn in conns:
        response = conn.recv_string()
        print("Benchmark output from one benchmark server: ")
        print(str(response))
        print()

    print('Finished!')

if __name__ == '__main__':
    main()