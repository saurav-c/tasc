#!/usr/bin/env python3

import zmq
import util

# Routing Ports
MEMBERSHIP_PORT = 6400

def register(client, ips):
    rtr_ips = util.get_node_ips(client, selector='role=routing', tp='ExternalIP')
    for ip in ips:
        join_hash_ring(rtr_ips, ip, ip)

def deregister(client, ips):
    rtr_ips = util.get_node_ips(client, selector='role=routing', tp='ExternalIP')
    for ip in ips:
        depart_hash_ring(rtr_ips, ip, ip)

def join_hash_ring(routers, public_ip, private_ip):
    msg = create_membership_msg('join', public_ip, private_ip)
    send_msg(routers, msg)

def depart_hash_ring(routers, public_ip, private_ip):
    msg = create_membership_msg('depart', public_ip, private_ip)
    send_msg(routers, msg)

def create_membership_msg(event, public, private):
    return event + ':' + 'MEMORY' + ':' + public + ':' + private + ':' + '0'

def send_msg(routers, msg):
    context = zmq.Context(1)
    for router in routers:
        dst = 'tcp://' + router + ':' + str(MEMBERSHIP_PORT)
        sock = context.socket(zmq.PUSH)
        sock.setsockopt(zmq.LINGER, 0) # Allows for breakdown of socket
        sock.connect(dst)
        sock.send_string(msg)
