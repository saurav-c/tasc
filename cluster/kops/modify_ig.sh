#!/bin/bash

YML_FILE=yaml/igs/$1-ig.yml

sed "s|CLUSTER_NAME|$TASC_CLUSTER_NAME|g" $YML_FILE > tmp.yml
sed -i "s|FILLER|$2|g" tmp.yml

kops replace -f tmp.yml --force > /dev/null 2>&1
rm tmp.yml

kops update cluster --name ${TASC_CLUSTER_NAME} --yes > /dev/null 2>&1
