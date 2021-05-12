#!/usr/bin/env python3

import os
import sys
from add_nodes import add_nodes
from remove_nodes import delete_nodes
import util
from routing_util import register, deregister
import subprocess
import time
import zmq

# AWS Info
aws_key_id = util.check_or_get_env_arg('AWS_ACCESS_KEY_ID')
aws_key = util.check_or_get_env_arg('AWS_SECRET_ACCESS_KEY')

# Config File Info
BASE_CONFIG_FILE = '../config/tasc-base.yml'
CONFIG_FILE = './tasc-config.yml'
POD_CONFIG_DIR = '/go/src/github.com/saurav-c/tasc/config'

NODE_TYPES = ['tasc', 'keynode', 'routing', 'lb', 'worker', 'benchmark']
client, apps_client = util.init_k8s()

# CLUSTER INIT INFO
LB_CONFIG_PORT = 15000
KEY_STANDBY_FILE = 'key-standby.txt'

def main():
    args = sys.argv[1:]
    cmd = args[0]

    if cmd == 'send-conf':
        ip = args[1]
        conf = args[2] if len(args) > 2 else None
        sendConfig(ip, conf)
    elif cmd == 'add':
        ntype = args[1]
        count = int(args[2])
        if ntype not in NODE_TYPES:
            print('Unknown node type: ' + ntype)
            return
        add(ntype, count)
    elif cmd == 'delete':
        ntype = args[1]
        count = int(args[2])
        if ntype not in NODE_TYPES:
            print('Unknown node type: ' + ntype)
            return
        delete(ntype, count)
    elif cmd == 'hashring':
        event = args[1]
        private_ip = args[2]
        hash_ring_change(event, private_ip)
    elif cmd == 'restart':
        node = args[1]
        kind = args[2]
        if node == 'all':
            restart_all(kind)
        else:
            restart(node, kind)
    elif cmd == 'get-stats':
        fetch_stats()
    elif cmd == 'clean-stats':
        clean_stats()
    elif cmd == 'clear':
        if len(args) > 1 and args[1] == 'cluster':
            ip = args[2]
            clear(cluster=True, anna_ip=ip)
        else:
            clear()
    elif cmd == 'cluster-init':
        txn = args[1]
        key = args[2]
        worker = args[3]
        cluster_init(txn, key, worker)
    else:
        print('Unknown cmd: ' + cmd)

# Restart pod with IP
def restart(pod_ip, kind):
    pod = util.get_pod_from_ip(client, pod_ip)
    pname = pod.metadata.name
    cname = pod.spec.containers[0].name
    kill_cmd = 'kubectl exec -it %s -c %s -- /sbin/killall5' % (pname, cname)
    subprocess.run(kill_cmd, shell=True)

    # Wait for pod to start again
    pod_ips = util.get_pod_ips(client, selector='role='+kind, is_running=True)
    while pod_ip not in pod_ips:
        pod_ips = util.get_pod_ips(client, selector='role='+kind, is_running=True)

    # Send kube config to lb
    if kind == 'lb':
        pod = util.get_pod_from_ip(client, pod_ip)
        send_kube_config(pod)

    # Send config file to the pod
    retry = 0
    while True:
        try:
            sendConfig(pod_ip, None)
            break
        except Exception as e:
            retry += 1
            print('Caught exception')
            if retry >= 5:
                print('Out of retries...')
                print(e)
                return
            print('Retrying in %d sec' % (retry * 10))
            time.sleep(retry * 10)

    print('Restarted %s node at %s' % (kind, pod_ip))

def send_kube_config(pod):
    kubecfg = os.path.join(os.environ['HOME'], '.kube/config')
    cname = pod.spec.containers[0].name
    retry = 0
    while True:
        try:
            util.copy_file_to_pod(client, kubecfg, pod.metadata.name,
                                  '/root/.kube', cname)
            break
        except Exception as e:
            retry += 1
            print('Caught exception')
            if retry >= 5:
                print('Out of retries...')
                print(e)
                return
            print('Retrying in %d sec' % (retry * 10))
            time.sleep(retry * 10)

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
              aws_key_id, aws_key, False, './', 'master')

    if prev_pod_ips is not None:
        pod_ips = util.get_pod_ips(client, 'role=' + kind, is_running=True)
        while len(pod_ips) != len(prev_pod_ips) + count:
            pod_ips = util.get_pod_ips(client, 'role=' + kind, is_running=True)

        created_pod_ips = list(set(pod_ips) - set(prev_pod_ips))

        # Register new keynodes with routers
        if kind == 'keynode':
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

def fetch_stats():
    mpod = client.list_namespaced_pod(namespace=util.NAMESPACE,
                                      label_selector='role=monitor').items[0]
    mmpname = mpod.metadata.name

    tasc_ips = util.get_pod_ips(client, selector='role=tasc', is_running=True)
    key_ips = util.get_pod_ips(client, selector='role=keynode', is_running=True)
    worker_ips = util.get_pod_ips(client, selector='role=worker', is_running=True)

    if not os.path.exists("stats"):
        os.makedirs("stats")

    tasc_ips = ["txn-manager_" + ip for ip in tasc_ips]
    key_ips = ["key-node_" + ip for ip in key_ips]
    worker_ips = ["worker_" + ip for ip in worker_ips]

    nodes = []
    nodes.extend(tasc_ips)
    nodes.extend(key_ips)
    nodes.extend(worker_ips)

    for node in nodes:
        cmd = 'kubectl cp default/%s:/go/src/github.com/saurav-c/tasc/cmd/monitor/stats/%s stats/%s' % (mmpname, node, node)
        subprocess.run(cmd, shell=True)

def clean_stats():
    dir = '/go/src/github.com/saurav-c/tasc/cmd/monitor/stats'

    mpod = client.list_namespaced_pod(namespace=util.NAMESPACE,
                                      label_selector='role=monitor').items[0]
    mmpname = mpod.metadata.name
    cname = mpod.spec.containers[0].name

    cmd = 'kubectl exec -it %s -c %s -- rm %s/*' % (mmpname, cname, dir)

    print('Monitor stats cleared')


def clear(cluster=False, anna_ip=None):
    context = zmq.Context(1)
    for role in ['tasc', 'keynode']:
        node_ips = util.get_node_ips(client, selector='role='+role, tp='ExternalIP')
        for ip in node_ips:
            dst = 'tcp://' + ip + ':15000'
            sock = context.socket(zmq.PUSH)
            sock.connect(dst)
            sock.send_string("")
            print('Cleared {} node at {}'.format(role, ip))
        print('Cleared all {} nodes'.format(role))

    clean_stats()

    print('Cleared all TASC components')

    if cluster:
        print('Clearing Anna Nodes...')
        dst = 'tcp://' + anna_ip + ':5000'
        sock = context.socket(zmq.REQ)
        sock.connect(dst)
        sock.send_string('CLEAR')
        sock.recv_string()
        print('Cleared Anna Nodes!!!')

def cluster_init(txn, key, worker, anna_ip):
    # Reconig transaction managers
    context = zmq.Context(1)
    lb_ips = util.get_node_ips(client, selector='role=lb', tp='ExternalIP')
    for ip in lb_ips:
        dst = 'tcp://' + ip + ':' + str(LB_CONFIG_PORT)
        sock = context.socket(zmq.PUSH)
        sock.connect(dst)
        sock.send_string(str(txn))
        print('Sent txn manager reconfig to load balancer at %s' % ip)

    # Reconfig key nodes
    key_ips = util.get_pod_ips(client, selector='role=keynode', is_running=True)
    standby_keys = []
    if os.path.exists(KEY_STANDBY_FILE):
        try:
            with open(KEY_STANDBY_FILE, 'r') as f:
                standby_keys = [x.strip() for x in f.readlines()]
        except Exception:
            pass

    active_key_count = len(key_ips) - len(standby_keys)
    if key > active_key_count:
        diff = key - active_key_count
        register_keys = standby_keys[:diff]
        register(client, register_keys)
        with open(KEY_STANDBY_FILE, 'w+') as f:
            for still_standby in standby_keys[diff:]:
                f.write(still_standby + '\n')
        print('Registered %d Key Nodes' % diff)
    elif key < active_key_count:
        diff = active_key_count - key
        deregister_keys = key_ips[:diff]
        deregister(client, deregister_keys)
        with open(KEY_STANDBY_FILE, 'w+') as f:
            for new_standby in deregister_keys + standby_keys:
                f.write(new_standby + '\n')
        print('Deregistered %d Key Nodes' % diff)

    # Clear Cluster
    clear(True, anna_ip)

    # Reconfig workers
    worker_ips = util.get_pod_ips(client, selector='role=worker', is_running=True)
    worker_count = len(worker_ips)
    if worker_count == worker:
        return False

    if worker > worker_count:
        add('worker', worker - worker_count)
        # cmd = 'python3 tools.py add worker {}'.format(worker - worker_count)
        # subprocess.Popen(cmd, cwd='/home/ec2-user/tasc/cluster')
        #
        # Wait
        print('Waiting for workers to start...')
        worker_ips = util.get_pod_ips(client, selector='role=worker', is_running=True)
        while len(worker_ips) < worker:
            worker_ips = util.get_pod_ips(client, selector='role=worker', is_running=True)
        return True
    elif worker < worker_count:
        # input("Delete {} workers to continue, waiting...".format(worker_count - worker))
        # cmd = 'python3 tools.py delete worker {}'.format(worker_count - worker)
        # subprocess.Popen(cmd, cwd='/home/ec2-user/tasc/cluster')
        #
        # # Wait
        delete('worker', worker_count - worker)
        print('Waiting for workers to terminate...')
        worker_ips = util.get_pod_ips(client, selector='role=worker', is_running=True)
        while len(worker_ips) > worker:
            worker_ips = util.get_pod_ips(client, selector='role=worker', is_running=True)
    return False

if __name__ == '__main__':
    main()