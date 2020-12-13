#!/bin/bash

echo "Validating cluster..."
kops validate cluster --name ${TASC_CLUSTER_NAME} > /dev/null 2>&1
while [ $? -ne 0 ]
do
  kops validate cluster --name ${TASC_CLUSTER_NAME} > /dev/null 2>&1
done

