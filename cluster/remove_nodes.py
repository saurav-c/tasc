#!/usr/bin/env python3

import os

import boto3
import kubernetes as k8s

import util
from routing_util import deregister

KOPS_DIR = '/home/ec2-user/tasc/cluster/kops'

ec2_client = boto3.client('ec2', os.getenv('AWS_REGION', 'us-east-1'))


def remove_node(ip, ntype):
    client, _ = util.init_k8s()

    pod = util.get_pod_from_ip(client, ip)
    hostname = 'ip-%s.ec2.internal' % (ip.replace('.', '-'))

    podname = pod.metadata.name
    client.delete_namespaced_pod(name=podname, namespace=util.NAMESPACE,
                                 body=k8s.client.V1DeleteOptions())
    client.delete_node(name=hostname, body=k8s.client.V1DeleteOptions())

    prev_count = util.get_previous_count(client, ntype)
    util.run_process(['./modify_ig.sh', ntype, str(prev_count - 1)], KOPS_DIR)


def delete_nodes(client, kind, count):
    print('Deleting %d %s server node(s) to cluster...' % (count, kind))

    prev_count = util.get_previous_count(client, kind)
    if prev_count < count:
        print('There are only %d %s server node(s)' % (prev_count, kind))
        return

    pods = client.list_namespaced_pod(namespace=util.NAMESPACE,
                                      label_selector='role=' +
                                                     kind).items
    pods_to_delete = pods[:count]
    deleted_pod_ips = []
    for pod in pods_to_delete:
        deleted_pod_ips.append(pod.status.pod_ip)
        podname = pod.metadata.name
        hostname = 'ip-%s.ec2.internal' % (pod.status.pod_ip.replace('.', '-'))
        client.delete_namespaced_pod(name=podname, namespace=util.NAMESPACE,
                                     body=k8s.client.V1DeleteOptions())
        client.delete_node(name=hostname, body=k8s.client.V1DeleteOptions())

    util.run_process(['./modify_ig.sh', kind, str(prev_count - count)], KOPS_DIR)

    # Notify routers about deleted key nodes
    if kind == 'keynode':
       deregister(client, deleted_pod_ips)
