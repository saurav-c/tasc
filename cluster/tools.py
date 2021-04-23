#!/usr/bin/env python3

import os
import sys
from add_nodes import add_nodes
from remove_nodes import delete_nodes
import util
from routing_util import register, deregister
import subprocess

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
        private_ip = args[3]
        hash_ring_change(event, private_ip)
    elif cmd == 'restart':
        node = args[1]
        kind = args[2]
        if node == 'all':
            restart_all(kind)
        else:
            restart(node, kind)
    else:
        print('Unknown cmd: ' + cmd)

# Restart pod with IP
def restart(pod_ip, kind):
    pod = util.get_pod_from_ip(pod_ip)
    pname = pod.metadata.name
    cname = pod.spec.containers[0].name
    kill_cmd = 'kubectl exec -it %s -c %s -- /bin/sh -c "kill 1"' % (pname, cname)
    subprocess.run(kill_cmd)

    # Wait for pod to start again
    pod_ips = util.get_pod_ips(client, selector='role='+kind, is_running=True)
    while pod_ip not in pod_ips:
        pod_ips = util.get_pod_ips(client, selector='role='+kind, is_running=True)

    # Send config file to the pod
    sendConfig(pod_ip, None)

    print('Restarted %s node at %s' % (kind, pod_ip))

def restart_all(kind):
    pod_ips = util.get_pod_ips(client, selector='role='+kind, is_running=True)
    for pod_ip in pod_ips:
        restart(pod_ip, kind)


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
    prev_pod_ips = None
    if kind == 'keynode':
        prev_pod_ips = util.get_pod_ips(client, 'role=' + kind, is_running=True)

    add_nodes(client, apps_client, BASE_CONFIG_FILE, kind, count,
              aws_key_id, aws_key, True, './', 'master')

    if prev_pod_ips is not None:
        pod_ips = util.get_pod_ips(client, 'role=' + kind, is_running=True)
        while len(pod_ips) != len(prev_pod_ips) + count:
            pod_ips = util.get_pod_ips(client, 'role=' + kind, is_running=True)

        created_pod_ips = list(set(pod_ips) - set(prev_pod_ips))

        # Register new keynodes with routers
        if kind == 'keynodes':
            register(client, created_pod_ips)

def delete(kind, count):
    delete_nodes(client, kind, count)

def hash_ring_change(event, private):
    if event == 'join':
        register(client, [private])
    elif event == 'depart':
        deregister(client, [private])
    else:
        print('Unknown event %s' % (event))



if __name__ == '__main__':
    main()