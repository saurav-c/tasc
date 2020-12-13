#!/usr/bin/env python3

import argparse
import os
import boto3
from add_nodes import add_nodes
import util

ec2_client = boto3.client('ec2', os.getenv('AWS_REGION', 'us-east-1'))

def create_cluster(txn_count, keynode_count, lb_count, benchmark_count, config_file, 
            ssh_key, cluster_name, kops_bucket, aws_key_id, aws_key):
    prefix = './'
    util.run_process(['./create_cluster_object.sh', kops_bucket, ssh_key], 'kops')

    client, apps_client = util.init_k8s()

    print('Creating Monitor Node...')
    add_nodes(client, apps_client, config_file, "monitor", 1,
              aws_key_id, aws_key, True, prefix)

    print('Creating %d Key Nodes...' % (keynode_count))
    add_nodes(client, apps_client, config_file, "keynode", keynode_count, aws_key_id,
    aws_key, True, prefix)

    print('Creating Keynode Router...')
    add_nodes(client, apps_client, config_file, "keyrouter", 1,
              aws_key_id, aws_key, True, prefix)
    
    print('Creating %d Load Balancer Nodes...' % lb_count)
    add_nodes(client, apps_client, config_file, 'lb', lb_count,
             aws_key_id, aws_key, True, prefix)

    lb_pods = client.list_namespaced_pod(namespace=util.NAMESPACE,
                                         label_selector="role=lb").items
    kubecfg = os.path.join(os.environ['HOME'], '.kube/config')
    for pod in lb_pods:
        util.copy_file_to_pod(client, kubecfg, pod.metadata.name,
                              '/root/.kube', 'lb-container')

    print('Creating %d TASC nodes...' % (txn_count))
    add_nodes(client, apps_client, config_file, 'tasc', txn_count,
              aws_key_id, aws_key, True, prefix)

    print('Creating %d Benchmark nodes...' % (benchmark_count))
    add_nodes(client, apps_client, config_file, 'benchmark', benchmark_count,
              aws_key_id, aws_key, True, prefix)

    benchmark_ips = util.get_node_ips(client, 'role=benchmark', 'ExternalIP')
    with open('../benchmark/benchmarks.txt', 'w+') as f:
        for ip in benchmark_ips:
            f.write(ip + '\n')

    print('Finished creating all pods...')

    # Create the Transaction Router
    print('Creating TASC service...')
    service_spec = util.load_yaml('yaml/services/tasc.yml', prefix)
    client.create_namespaced_service(namespace=util.NAMESPACE,
                                     body=service_spec)

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

    print("The TASC LB Endpoint: " + util.get_service_address(client, "tasc-service"))
    print('Finished!')


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
    parser.add_argument('-l', '--loadbalancer', nargs=1, type=int, metavar='L',
                        help='The number of load balancer nodes to start with ' +
                             '(required)', dest='loadbalancer', required=True)
    parser.add_argument('-b', '--benchmark', nargs=1, type=int, metavar='L',
                        help='The number of benchmark nodes to start with ' +
                             '(required)', dest='benchmark', required=True)
    parser.add_argument('--config', nargs='?', type=str,
                        help='The configuration file to start the cluster with'
                        + ' (optional)', dest='config',
                        default='../config/tasc-base.yml')
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

    create_cluster(args.nodes[0], args.keynodes[0], args.loadbalancer[0], 
                args.benchmark[0], args.config, args.sshkey, cluster_name, 
                kops_bucket, aws_key_id, aws_key)
