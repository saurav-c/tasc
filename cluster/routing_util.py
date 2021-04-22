#!/usr/bin/env python3

import zmq

# Manager Ports
MNG_JOIN_PORT = 5000
MNG_DEPART_PORT = 5001

# Routing Ports
MEMBERSHIP_PORT = 6400

# Send register message to manager
def register(manager, private_ip, public_ip=None):
    context = zmq.Context(1)
    dst = 'tcp://' + manager + ':' + str(MNG_JOIN_PORT)
    sock = context.socket(zmq.PUSH)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(dst)
    public_ip = private_ip if not public_ip else public_ip
    msg += public_ip
    msg += ':'
    msg += private_ip
    sock.send_string(msg)

# Send deregister message to manager
def deregister(manager, private_ip, public_ip=None):
    context = zmq.Context(1)
    dst = 'tcp://' + manager + ':' + str(MNG_DEPART_PORT)
    sock = context.socket(zmq.PUSH)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(dst)
    public_ip = private_ip if not public_ip else public_ip
    msg += public_ip
    msg += ':'
    msg += private_ip
    sock.send_string(msg)

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
