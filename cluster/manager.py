#!/usr/bin/env python3

import util
import routing_util as ru
import zmq
import logging

logging.basicConfig(filename='manager.log', filemode='w')

def main():
    client, apps_client = util.init_k8s()
    context = zmq.Context()

    # Sockets for hash ring membership changes
    rtr_join_sock = context.socket(zmq.PULL)
    rtr_join_sock.bind('tcp://*:%s' % (str(ru.MNG_JOIN_PORT)))
    rtr_depart_sock = context.socket(zmq.PULL)
    rtr_depart_sock.bind('tcp://*:%s' % (str(ru.MNG_DEPART_PORT)))

    poller = zmq.Poller()
    poller.register(rtr_join_sock, zmq.POLLIN)

    while True:
        socks = dict(poller.poll())
        if rtr_join_sock in socks and socks[rtr_join_sock] == zmq.POLLIN:
            logging.info('Received join')
            msg = rtr_join_sock.recv()
            router_broadcast(client, 'join', msg)
        if rtr_depart_sock in socks and socks[rtr_depart_sock] == zmq.POLLIN:
            logging.info('Received depart')
            msg = rtr_depart_sock.recv()
            router_broadcast(client, 'depart', msg)

def router_broadcast(client, event, msg):
    args = msg.split(':')
    rtr_ips = util.get_pod_ips(client, 'role=routing', is_running=True)
    if event == 'join':
        ru.join_hash_ring(rtr_ips, args[0], args[1])
    elif event == 'depart':
        ru.depart_hash_ring(rtr_ips, args[0]. args[1])
    else:
        print('Unknown event %s' % (event))

if __name__ == '__main__':
    main()