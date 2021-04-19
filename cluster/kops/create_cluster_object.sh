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
kops create cluster --master-size c5.large --node-size c5.large --zones us-east-1a --ssh-public-key ${SSH_KEY}.pub ${TASC_CLUSTER_NAME} --networking kubenet

echo "Deleting default instance group..."
kops delete ig nodes --name ${TASC_CLUSTER_NAME} --yes

echo "Adding general instance group"
sed "s|CLUSTER_NAME|$TASC_CLUSTER_NAME|g" yaml/igs/general-ig.yml > tmp.yml
kops create -f tmp.yml
rm tmp.yml

echo "Creating cluster on AWS..."
kops update cluster --name ${TASC_CLUSTER_NAME} --yes

./validate_cluster.sh

