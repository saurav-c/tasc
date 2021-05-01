#!/usr/bin/env python3

import argparse
import datetime
import zmq
import json

def main():
    parser = argparse.ArgumentParser(description='Makes a call to the TASC benchmark server.')
    parser.add_argument('-c', '--clients', nargs=1, type=int, metavar='Y',
                        help='The number of clients to invoke.',
                        dest='clients', required=True)
    parser.add_argument('-l', '--lambda', nargs=1, type=str, metavar='Y',
                        help='The name of AWS Lambda Function to be run.', 
                        dest='awslambda', required=True)
    parser.add_argument('-a', '--address', nargs=1, type=str, metavar='A',
                        help='ELB Address for the Load Balancer Values.', 
                        dest='address', required=True)
    parser.add_argument('-t', '--txn', nargs=1, type=int, metavar='Y',
                        help='The number of txns to be done.',
                        dest='txn', required=True)
    parser.add_argument('-r', '--reads', nargs=1, type=int, metavar='Y',
                        help='The number of reads to be done.',
                        dest='reads', required=True)
    parser.add_argument('-w', '--writes', nargs=1, type=int, metavar='Y',
                        help='The number of writes to be done.',
                        dest='writes', required=True)
    parser.add_argument('-z', '--zipf', nargs=1, type=float, metavar='Y',
                        help='Zipfian coefficient',
                        dest='zipf', required=False, default=1.0)
    parser.add_argument('-p', '--pre', nargs=1, type=str, metavar='Y',
                        help='Prefix key',
                        dest='prefix', required=False, default='tasc')
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

        payload = {
            'lambda': args.awslambda[0],
            'num_clients': args.clients[0],
            'num_txns': args.txn[0],
            'num_reads': args.reads[0],
            'num_writes': args.writes[0],
            'elb': args.address[0],
            'zipf': args.zipf,
            'prefix': args.prefix
        }
        message = json.dumps(payload)

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