#!/usr/bin/env python3

import argparse
import os
import boto3
from add_nodes import add_nodes
import util

ec2_client = boto3.client('ec2', os.getenv('AWS_REGION', 'us-east-1'))

def create_cluster(txn_count, keynode_count, rtr_count, worker_count, lb_count, benchmark_count, config_file,
            branch_name, ssh_key, cluster_name, kops_bucket, aws_key_id, aws_key, anna_config_file):
    prefix = './'
    util.run_process(['./create_cluster_object.sh', kops_bucket, ssh_key], 'kops')

    client, apps_client = util.init_k8s()

    print('Creating Manager Node...')
    add_nodes(client, apps_client, config_file, "manager", 1,
              aws_key_id, aws_key, True, prefix, branch_name)

    mngr_pod_ips = util.get_pod_ips(client, 'role=manager', is_running=True)
    while len(mngr_pod_ips) < 1:
        mngr_pod_ips = util.get_pod_ips(client, 'role=manager', is_running=True)

    # Copy files to managers for kubectl
    mngr_pods = client.list_namespaced_pod(namespace=util.NAMESPACE,
                                         label_selector="role=manager").items
    copy_kube_config(client, mngr_pods)

    print('Creating Manager service...')
    service_spec = util.load_yaml('yaml/services/manager.yml', prefix)
    client.create_namespaced_service(namespace=util.NAMESPACE,
                                     body=service_spec)
    util.get_service_address(client, 'manager-service')

    print('Creating Monitor Node...')
    add_nodes(client, apps_client, config_file, "monitor", 1,
              aws_key_id, aws_key, True, prefix, branch_name)

    print('Creating %d Anna Routing Nodes...' % (rtr_count))
    add_nodes(client, apps_client, anna_config_file, "routing", rtr_count,
              aws_key_id, aws_key, True, prefix, branch_name)

    print('Creating routing service...')
    service_spec = util.load_yaml('yaml/services/routing.yml', prefix)
    client.create_namespaced_service(namespace=util.NAMESPACE,
                                     body=service_spec)
    util.get_service_address(client, 'routing-service')

    # Wait for all routing nodes to be ready
    routing_pods_ips = util.get_pod_ips(client, 'role=routing', is_running=True)
    while len(routing_pods_ips) < rtr_count:
        routing_pods_ips = util.get_pod_ips(client, 'role=routing', is_running=True)

    print('Creating %d Key Nodes...' % (keynode_count))
    add_nodes(client, apps_client, config_file, "keynode", keynode_count, aws_key_id,
              aws_key, True, prefix, branch_name)

    print('Creating %d Worker Nodes...' % (worker_count))
    add_nodes(client, apps_client, config_file, "worker", worker_count, aws_key_id,
              aws_key, True, prefix, branch_name)

    print('Creating Worker Service...')
    service_spec = util.load_yaml('yaml/services/worker.yml', prefix)
    client.create_namespaced_service(namespace=util.NAMESPACE,
                                     body=service_spec)
    util.get_service_address(client, 'worker-service')

    print('Creating %d TASC nodes...' % (txn_count))
    add_nodes(client, apps_client, config_file, 'tasc', txn_count,
              aws_key_id, aws_key, True, prefix, branch_name)

    print('Creating %d Load Balancers...' % (lb_count))
    add_nodes(client, apps_client, config_file, 'lb', lb_count,
              aws_key_id, aws_key, True, prefix, branch_name)
    
    lb_pod_ips = util.get_pod_ips(client, 'role=lb', is_running=True)
    while len(lb_pod_ips) < lb_count:
        lb_pod_ips = util.get_pod_ips(client, 'role=lb', is_running=True)

    # Copy files to load balancers for kubectl
    lb_pods = client.list_namespaced_pod(namespace=util.NAMESPACE,
                                         label_selector="role=lb").items
    copy_kube_config(client, lb_pods)

    print('Creating TASC Load Balancing service...')
    service_spec = util.load_yaml('yaml/services/tasc.yml', prefix)
    client.create_namespaced_service(namespace=util.NAMESPACE,
                                     body=service_spec)

    # print('Creating %d Benchmark nodes...' % (benchmark_count))
    # add_nodes(client, apps_client, config_file, 'benchmark', benchmark_count,
    #           aws_key_id, aws_key, True, prefix, branch_name)
    #
    # benchmark_ips = util.get_node_ips(client, 'role=benchmark', 'ExternalIP')
    # with open('../cmd/benchmark/benchmarks.txt', 'w+') as f:
    #     for ip in benchmark_ips:
    #         f.write(ip + '\n')

    print('Finished creating all pods...')

    sg_name = 'nodes.' + cluster_name
    sg = ec2_client.describe_security_groups(
          Filters=[{'Name': 'group-name',
                    'Values': [sg_name]}])['SecurityGroups'][0]
    print("Authorizing Ports for TASC...")
    permission = [{
        'FromPort': 0,
        'IpProtocol': 'tcp',
        'ToPort': 65535,
        'IpRanges': [{
            'CidrIp': '0.0.0.0/0'
        }]
    }]

    ec2_client.authorize_security_group_ingress(GroupId=sg['GroupId'],
                                                IpPermissions=permission)

    print("\nThe TASC ELB Endpoint: " + util.get_service_address(client, "tasc-service") + "\n")
    print('Finished!')

def copy_kube_config(client, pods):
    kubecfg = os.path.join(os.environ['HOME'], '.kube/config')
    for pod in pods:
        cname = pod.spec.containers[0].name
        util.copy_file_to_pod(client, kubecfg, pod.metadata.name,
                              '/root/.kube', cname)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='''Creates a cluster
                                     using Kubernetes and kops. If no SSH key
                                     is specified, we use the default SSH key
                                     (~/.ssh/id_rsa), and we expect that the
                                     correponding public key has the same path
                                     and ends in .pub.

                                     If no configuration file base is
                                     specified, we use the default
                                     ($config/tasc-base.yaml).''')

    parser.add_argument('-n', '--nodes', nargs=1, type=int, metavar='N',
                        help='The number of TASC nodes to start with ' +
                        '(required)', dest='nodes', required=True)
    parser.add_argument('-k', '--keynodes', nargs=1, type=int, metavar='K',
                        help='The number of keynodes to start with ' +
                        '(required)', dest='keynodes', required=True)
    parser.add_argument('-w', '--workers', nargs=1, type=int, metavar='K',
                        help='The number of workers to start with ' +
                             '(required)', dest='workers', required=True)
    parser.add_argument('-r', '--routers', nargs=1, type=int, metavar='L',
                        help='The number of (Anna) router nodes to start with ' +
                             '(required)', dest='routers', required=True)
    parser.add_argument('-l', '--loadbalancer', nargs=1, type=int, metavar='L',
                        help='The number of load balancer nodes to start with ' +
                             '(required)', dest='lb', required=True)
    parser.add_argument('-b', '--benchmark', nargs=1, type=int, metavar='L',
                        help='The number of benchmark nodes to start with ' +
                             '(required)', dest='benchmark', required=True)
    parser.add_argument('--branch', nargs='?', type=str,
                            help='The branch to start the cluster with'
                            + ' (optional)', dest='branch',
                            default='master')
    parser.add_argument('--config', nargs='?', type=str,
                        help='The configuration file to start the cluster with'
                        + ' (optional)', dest='config',
                        default='../config/tasc-base.yml')
    parser.add_argument('--anna-config', nargs='?', type=str,
                        help='The configuration file for Anna routing cluster'
                             + ' (optional)', dest='annaconfig',
                        default='../../conf/anna-base.yml')
    parser.add_argument('--ssh-key', nargs='?', type=str,
                        help='The SSH key used to configure and connect to ' +
                        'each node (optional)', dest='sshkey',
                        default=os.path.join(os.environ['HOME'],
                                             '.ssh/id_rsa'))

    cluster_name = util.check_or_get_env_arg('TASC_CLUSTER_NAME')
    kops_bucket = util.check_or_get_env_arg('KOPS_STATE_STORE')
    aws_key_id = util.check_or_get_env_arg('AWS_ACCESS_KEY_ID')
    aws_key = util.check_or_get_env_arg('AWS_SECRET_ACCESS_KEY')

    args = parser.parse_args()

    create_cluster(args.nodes[0], args.keynodes[0], args.routers[0], args.workers[0],
                args.lb[0], args.benchmark[0], args.config,
                args.branch, args.sshkey, cluster_name,
                kops_bucket, aws_key_id, aws_key, args.annaconfig)
