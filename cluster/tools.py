#!/usr/bin/env python3

import os
import sys
from add_nodes import add_nodes
from remove_nodes import delete_nodes
import util
from routing_util import register, deregister

# AWS Info
aws_key_id = util.check_or_get_env_arg('AWS_ACCESS_KEY_ID')
aws_key = util.check_or_get_env_arg('AWS_SECRET_ACCESS_KEY')

# Config File Info
BASE_CONFIG_FILE = '../config/tasc-base.yml'
CONFIG_FILE = './tasc-config.yml'
POD_CONFIG_DIR = '/go/src/github.com/saurav-c/tasc/config'

NODE_TYPES = ['tasc', 'keynode', 'routing', 'lb', 'worker']
client, apps_client = util.init_k8s()

def main():
    args = sys.argv[1:]
    cmd = args[0]

    if cmd == 'send-conf':
        ip = args[1]
        conf = args[2] if len(args) > 2 else None
        sendConfig(ip, conf)
    elif cmd == 'add':
        ntype = args[1]
        count = args[2]
        if ntype not in NODE_TYPES:
            print('Unknown node type: ' + ntype)
            return
        add(ntype, count)
    elif cmd == 'delete':
        ntype = args[1]
        count = args[2]
        if ntype not in NODE_TYPES:
            print('Unknown node type: ' + ntype)
            return
        delete(ntype, count)
    elif cmd == 'hashring':
        event = args[1]
        public_ip = args[2]
        private_ip = args[3]
        hash_ring_change(event, public_ip, private_ip)
    else:
        print('Unknown cmd: ' + cmd)

# Sends config file to node at NODEIP
def sendConfig(nodeIP, configFile):
    pod = util.get_pod_from_ip(client, nodeIP)
    pname = pod.metadata.name
    # There is only 1 container in each Pod
    cname = pod.spec.containers[0].name

    cfile = configFile if configFile else BASE_CONFIG_FILE
    os.system(str('cp %s ' + CONFIG_FILE) % cfile)

    util.copy_file_to_pod(client, CONFIG_FILE[2:], pname,
                          POD_CONFIG_DIR, cname)

    os.system('rm ' + CONFIG_FILE)

def add(kind, count):
    add_nodes(client, apps_client, BASE_CONFIG_FILE, kind, count,
              aws_key_id, aws_key, True, './', 'master')

def delete(kind, count):
    delete_nodes(client, kind, count)

def hash_ring_change(event, public, private):
    manager_svc = util.get_service_address(client, 'manager-service')
    if event == 'join':
        register(manager_svc, public, private)
    elif event == 'depart':
        deregister(manager_svc, public, private)
    else:
        print('Unknown event %s' % (event))



if __name__ == '__main__':
    main()