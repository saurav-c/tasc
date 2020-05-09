#!/usr/bin/env python3

import argparse
import os
import boto3
from add_nodes import add_nodes
import util

ec2_client = boto3.client('ec2', os.getenv('AWS_REGION', 'us-east-1'))

def create_cluster(txn_count, keynode_count, bench_count, cfile,
                   ssh_key, cluster_name, kops_bucket, aws_key_id, aws_key):
    prefix = './'
    util.run_process(['./create_cluster_object.sh', kops_bucket, ssh_key])
    util.run_process(['./validate_cluster.sh'])

    client, apps_client = util.init_k8s()

    print('Creating %d key nodes...' % (keynode_count))
    util.run_process(['./modify_ig.sh', 'keynode', "%d" % keynode_count])
    add_nodes(client, apps_client, cfile, "keynode", keynode_count, aws_key_id,
    aws_key, True, prefix)

    # Figure out how to add key_ips to the config.yaml file

    print('Creating keynode router')
    add_nodes(client, apps_client, cfile, "keyrouter", 1,
              aws_key_id, aws_key, True, prefix)

    print('Creating %d TASC nodes...' % (txn_count))
    util.run_process(['./modify_ig.sh', 'tasc', "%d" % txn_count])
    add_nodes(client, apps_client, cfile, 'tasc', txn_count,
              aws_key_id, aws_key, True, prefix)
    util.get_pod_ips(client, 'role=aft')

    tasc_ips = util.get_node_ips(client, 'role=aft', 'ExternalIP')

    # Create the txn router
    print("Creating txn router")
    add_nodes(client, apps_client, cfile, "txnrouter", 1,
              aws_key_id, aws_key, True, prefix)


    print('Finished creating all pods...')

    print('Creating TASC service...')
    service_spec = util.load_yaml('yaml/services/tasc.yml', prefix)
    client.create_namespaced_service(namespace=util.NAMESPACE,
                                     body=service_spec)

    sg_name = 'nodes.' + cluster_name
    sg = ec2_client.describe_security_groups(
          Filters=[{'Name': 'group-name',
                    'Values': [sg_name]}])['SecurityGroups'][0]

    print('Authorizing ports for Aft replicas...')
    permission = [{
        'FromPort': 6000,
        'IpProtocol': 'tcp',
        'ToPort': 9000,
        'IpRanges': [{
            'CidrIp': '0.0.0.0/0'
        }]
    }, {
        'FromPort': 6001,
        'IpProtocol': 'tcp',
        'ToPort': 9001,
        'IpRanges': [{
            'CidrIp': '0.0.0.0/0'
        }]
    }, {
        'FromPort': 6002,
        'IpProtocol': 'tcp',
        'ToPort': 9002,
        'IpRanges': [{
            'CidrIp': '0.0.0.0/0'
        }]
    }, {
        'FromPort': 5001,
        'IpProtocol': 'tcp',
        'ToPort': 5002,
        'IpRanges': [{
            'CidrIp': '0.0.0.0/0'
        }]
    },  {
        'FromPort': 9000,
        'IpProtocol': 'tcp',
        'ToPort': 6000,
        'IpRanges': [{
            'CidrIp': '0.0.0.0/0'
        }]
    }, {
        'FromPort': 9001,
        'IpProtocol': 'tcp',
        'ToPort': 6001,
        'IpRanges': [{
            'CidrIp': '0.0.0.0/0'
        }]
    }, {
        'FromPort': 9002,
        'IpProtocol': 'tcp',
        'ToPort': 6002,
        'IpRanges': [{
            'CidrIp': '0.0.0.0/0'
        }]
    } ]
    ec2_client.authorize_security_group_ingress(GroupId=sg['GroupId'],
                                                IpPermissions=permission)
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
                                     ($config/tasc-base.yml).''')

    parser.add_argument('-r', '--replicas', nargs=1, type=int, metavar='R',
                        help='The number of Aft replicas to start with ' +
                        '(required)', dest='replicas', required=True)
    parser.add_argument('-g', '--gc', nargs=1, type=int, metavar='G',
                        help='The number of GC replicas to start with ' +
                        '(required)', dest='gc', required=True)
    parser.add_argument('-l', '--lb', nargs=1, type=int, metavar='L',
                        help='The number of LB replicas to start with ' +
                        '(required)', dest='lb', required=True)
    parser.add_argument('-b', '--benchmark', nargs='?', type=int, metavar='B',
                        help='The number of benchmark nodes in the cluster ' +
                        '(optional)', dest='benchmark', default=0)
    parser.add_argument('--conf', nargs='?', type=str,
                        help='The configuration file to start the cluster with'
                        + ' (optional)', dest='conf',
                        default='../config/tasc-base.yml')
    parser.add_argument('--ssh-key', nargs='?', type=str,
                        help='The SSH key used to configure and connect to ' +
                        'each node (optional)', dest='sshkey',
                        default=os.path.join(os.environ['HOME'],
                                             '.ssh/id_rsa'))

    cluster_name = util.check_or_get_env_arg('HYDRO_CLUSTER_NAME')
    kops_bucket = util.check_or_get_env_arg('KOPS_STATE_STORE')
    aws_key_id = util.check_or_get_env_arg('AWS_ACCESS_KEY_ID')
    aws_key = util.check_or_get_env_arg('AWS_SECRET_ACCESS_KEY')

    args = parser.parse_args()

    create_cluster(args.replicas[0], args.gc[0], args.lb[0], args.benchmark,
                   args.conf, args.sshkey, cluster_name, kops_bucket,
                   aws_key_id, aws_key)
