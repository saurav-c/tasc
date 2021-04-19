#!/bin/bash

# A helper function that takes a space separated list and generates a string
# that parses as a YAML list.
gen_yml_list() {
  IFS=' ' read -r -a ARR <<< $1
  RESULT=""

  for IP in "${ARR[@]}"; do
    RESULT=$"$RESULT        - $IP\n"
  done

  echo -e "$RESULT"
}

# Create the AWS access key infrastructure.
mkdir -p ~/.aws
echo -e "[default]\nregion = us-east-1" > ~/.aws/config
echo -e "[default]\naws_access_key_id = $AWS_ACCESS_KEY_ID\naws_secret_access_key = $AWS_SECRET_ACCESS_KEY" > ~/.aws/credentials

# Establish IP Addresses
PRIVATE_IP=`ifconfig eth0 | grep 'inet' | grep -v inet6 | sed -e 's/^[ \t]*//' | cut -d' ' -f2`
PUBLIC_IP=`curl http://169.254.169.254/latest/meta-data/public-ipv4`

if [[ "$ROLE" = "lb" ]]; then
  mkdir -p /root/.kube
fi

# Fetch most recent version of code
cd $TASC_HOME
git fetch -p origin
git checkout -b origin/${BRANCH}

if [[ "$ROLE" = "manager" ]]; then
  mkdir -p /root/.kube
fi

# Wait for the config file to be passed in.
while [[ ! -f $TASC_HOME/config/tasc-config.yml ]]; do
  sleep 1
done

# Generate the YML config file.
echo "privateIP: $PRIVATE_IP" >> config/tasc-config.yml
echo "publicIP: $PUBLIC_IP" >> config/tasc-config.yml

# Start the process.
if [[ "$ROLE" = "tasc" ]]; then
  echo "monitorIP: $MONITOR" >> config/tasc-config.yml
  echo "routingILB: $ROUTING_ILB" >> config/tasc-config.yml
  cd $TASC_HOME/cmd/manager
  go build
  ./manager
elif [[ "$ROLE" = "keynode" ]]; then
  echo "monitorIP: $MONITOR" >> config/tasc-config.yml
  cd $TASC_HOME/cmd/keynode
  go build
  ./keynode
elif [[ "$ROLE" = "monitor" ]]; then
  cd $TASC_HOME/cmd/monitor
  go build
  ./monitor
elif [[ "$ROLE" = "benchmark" ]]; then
  cd $TASC_HOME/cmd/benchmark
  go build
  python3 benchmark_server.py
fi