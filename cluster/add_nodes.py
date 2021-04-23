#!/usr/bin/env python3

import os
import boto3
import util

ec2_client = boto3.client('ec2', os.getenv('AWS_REGION', 'us-east-1'))


def add_nodes(client, apps_client, cfile, kind, count, aws_key_id=None,
              aws_key=None, create=False, prefix=None, branch="master"):
    print('Adding %d %s server node(s) to cluster...' % (count, kind))

    prev_count = util.get_previous_count(client, kind)
    util.run_process(['./modify_ig.sh', kind, str(count + prev_count)], 'kops')

    util.run_process(['./validate_cluster.sh'], 'kops')

    if create:
        fname = 'yaml/ds/%s-ds.yml' % kind
        yml = util.load_yaml(fname, prefix)

        for container in yml['spec']['template']['spec']['containers']:
            env = container['env']
            util.replace_yaml_val(env, 'BRANCH', branch)
            util.replace_yaml_val(env, 'AWS_ACCESS_KEY_ID', aws_key_id)
            util.replace_yaml_val(env, 'AWS_SECRET_ACCESS_KEY', aws_key)
            if kind == "tasc":
                routing_svc = util.get_service_address(client, 'routing-service')
                util.replace_yaml_val(env, 'ROUTING_ILB', routing_svc)
                monitor_ip = util.get_node_ips(client, 'role=monitor', 'ExternalIP')[0]
                util.replace_yaml_val(env, 'MONITOR', monitor_ip)
                worker_svc = util.get_service_address(client, 'worker-service')
                util.replace_yaml_val(env, 'WORKER_ILB', worker_svc)
            if kind == "keynode":
                monitor_ip = util.get_node_ips(client, 'role=monitor', 'ExternalIP')[0]
                util.replace_yaml_val(env, 'MONITOR', monitor_ip)
            if kind == 'worker':
                monitor_ip = util.get_node_ips(client, 'role=monitor', 'ExternalIP')[0]
                util.replace_yaml_val(env, 'MONITOR', monitor_ip)
                routing_svc = util.get_service_address(client, 'routing-service')
                util.replace_yaml_val(env, 'ROUTING_ILB', routing_svc)

        apps_client.create_namespaced_daemon_set(namespace=util.NAMESPACE,
                                                 body=yml)

        # Wait until all pods of this kind are running
        res = []
        while len(res) != count:
            res = util.get_pod_ips(client, 'role=' + kind, is_running=True)

        created_pods = []
        pods = client.list_namespaced_pod(namespace=util.NAMESPACE,
                                          label_selector='role=' +
                                                         kind).items

        # Send kube config to lb
        if kind == 'lb':
            kubecfg = os.path.join(os.environ['HOME'], '.kube/config')
            for pod in pods:
                cname = pod.spec.containers[0].name
                util.copy_file_to_pod(client, kubecfg, pod.metadata.name,
                                      '/root/.kube', cname)

        # Generate list of all recently created pods.
        created_pod_ips = []
        for pod in pods:
            created_pod_ips.append(pod.status.pod_ip)
            pname = pod.metadata.name
            for container in pod.spec.containers:
                cname = container.name
                created_pods.append((pname, cname))

        # Copy the KVS config into all recently created pods.
        cfile_name = './tasc-config.yml' if kind != 'routing' else './anna-config.yml'
        cfile_dir = '/go/src/github.com/saurav-c/tasc/config' if kind != 'routing' else 'hydro/anna/conf'
        os.system(str('cp %s ' + cfile_name) % cfile)

        for pname, cname in created_pods:
            util.copy_file_to_pod(client, cfile_name[2:], pname,
                                  cfile_dir, cname)
        os.system('rm ' + cfile_name)
