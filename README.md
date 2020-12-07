# TASC: A Transactional Shim with Strong Consistency for Serverless Computing

[TASC](https://github.com/saurav-c/aftsi) is a serverless fault-tolerance and strong consistency shim that interposes between serverless compute and storage layers.

## Code Organization

The `cmd` directory contains all of the source code for TASC.

The `proto` directory has TASC-specific protobuf definitions and compiled protobufs for AFT (and Anna if necessary).

The `cluster` directory has scripts and YAML specs to spin up Kubernetes clusters that run multi-node versions of AFT. This is the recommended way to deploy the system.
