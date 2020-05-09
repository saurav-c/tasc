#!/bin/bash

if [[ -z "$1" ]] && [[ -z "$2" ]]; then
  echo "Usage: ./create_cluster_object.sh cluster-state-store path-to-ssh-key"
  echo ""
  echo "Cluster name and S3 Bucket used as kops state store must be specified."
  echo "If no SSH key is specified, the default SSH key (~/.ssh/id_rsa) will be used."
  exit 1
fi

if [[ -z "$AWS_ACCESS_KEY_ID" ]] || [[ -z "$AWS_SECRET_ACCESS_KEY" ]]; then
  echo "AWS access credentials are required to be stored in local environment variables for cluster creation."
  echo "Please use the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY variables."
  exit 1
fi

KOPS_STATE_STORE=$1
SSH_KEY=$2

echo "Creating cluster object..."
kops create cluster --master-size t2.micro --node-size t2.micro --zones us-east-1a --ssh-public-key ${SSH_KEY}.pub ${HYDRO_CLUSTER_NAME} --networking kube-router > /dev/null 2>&1
# delete default instance group that we won't use
kops delete ig nodes --name ${HYDRO_CLUSTER_NAME} --yes > /dev/null 2>&1

# create the cluster
echo "Creating cluster on AWS..."
kops update cluster --name ${HYDRO_CLUSTER_NAME} --yes > /dev/null 2>&1

./validate_cluster.sh
