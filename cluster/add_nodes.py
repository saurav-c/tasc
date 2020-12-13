#!/usr/bin/env python3

import os
import boto3
import util

ec2_client = boto3.client('ec2', os.getenv('AWS_REGION', 'us-east-1'))


def add_nodes(client, apps_client, cfile, kind, count, aws_key_id=None,
              aws_key=None, create=False, prefix=None):
    print('Adding %d %s server node(s) to cluster...' % (count, kind))

    prev_count = util.get_previous_count(client, kind)
    util.run_process(['./modify_ig.sh', kind, str(count + prev_count)], 'kops')

    util.run_process(['./validate_cluster.sh'], 'kops')

    if create:
        fname = 'yaml/ds/%s-ds.yml' % kind
        yml = util.load_yaml(fname, prefix)

        for container in yml['spec']['template']['spec']['containers']:
            env = container['env']
            util.replace_yaml_val(env, 'AWS_ACCESS_KEY_ID', aws_key_id)
            util.replace_yaml_val(env, 'AWS_SECRET_ACCESS_KEY', aws_key)
            if kind == "keyrouter":
                key_ips = util.get_node_ips(client, 'role=keynode', 'ExternalIP')
                keynodes = ' '.join(key_ips)  
                util.replace_yaml_val(env, 'NODE_IPS', keynodes)
            if kind == "tasc":
                keyrouter_ip = util.get_node_ips(client, 'role=keyrouter', 'ExternalIP')[0]
                util.replace_yaml_val(env, 'KEY_ROUTER', keyrouter_ip)
                monitor_ip = util.get_node_ips(client, 'role=monitor', 'ExternalIP')[0]
                util.replace_yaml_val(env, 'MONITOR', monitor_ip)
            if kind == "keynode":
                monitor_ip = util.get_node_ips(client, 'role=monitor', 'ExternalIP')[0]
                util.replace_yaml_val(env, 'MONITOR', monitor_ip)

        apps_client.create_namespaced_daemon_set(namespace=util.NAMESPACE,
                                                 body=yml)

        # Wait until all pods of this kind are running
        res = []
        while len(res) != count:
            res = util.get_pod_ips(client, 'role='+kind, is_running=True)

        created_pods = []
        pods = client.list_namespaced_pod(namespace=util.NAMESPACE,
                                          label_selector='role=' +
                                          kind).items

        # Generate list of all recently created pods.
        for pod in pods:
            pname = pod.metadata.name
            for container in pod.spec.containers:
                cname = container.name
                created_pods.append((pname, cname))

        # Copy the KVS config into all recently created pods.
        os.system('cp %s ./tasc-config.yml' % cfile)

        for pname, cname in created_pods:
            util.copy_file_to_pod(client, 'tasc-config.yml', pname,
                                  '/go/src/github.com/saurav-c/aftsi/config', cname)
        os.system('rm ./tasc-config.yml')
