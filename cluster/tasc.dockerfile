FROM ubuntu:18.04
MAINTAINER Taj Shaik <tajshaik24@gmail.com> version: 1.0

USER root
ENV GOPATH /go
ENV GOBIN /go/bin

# Setup the go dir.
RUN mkdir $GOPATH
RUN mkdir $GOPATH/bin
RUN mkdir $GOPATH/src
RUN mkdir $GOPATH/pkg

# Setting up ENV Variables
ENV PATH $PATH:$GOPATH/bin
ENV TASC_HOME $GOPATH/src/github.com/saurav-c/aftsi

# Install Go, other Ubuntu dependencies.
RUN apt-get update
RUN apt-get install -y software-properties-common
RUN add-apt-repository -y ppa:longsleep/golang-backports
RUN apt-get update
RUN apt-get install -y golang-go wget unzip git ca-certificates net-tools python3-pip libzmq3-dev dnsutils curl apt-transport-https

# Install kubectl for the management pod.
RUN curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
RUN echo "deb https://apt.kubernetes.io/ kubernetes-xenial main" | tee -a /etc/apt/sources.list.d/kubernetes.list
RUN apt-get update
RUN apt-get install -y kubectl

# Updates certificates, so go get works.
RUN update-ca-certificates

# Install protoc.
RUN wget https://github.com/protocolbuffers/protobuf/releases/download/v3.10.0/protoc-3.10.0-linux-x86_64.zip
RUN unzip protoc-3.10.0-linux-x86_64.zip -d /usr/local

# Install required Go dependencies.
RUN go get -u gopkg.in/yaml.v2
RUN go get -u google.golang.org/grpc
RUN go get -u github.com/golang/protobuf/protoc-gen-go
RUN go get -u github.com/pebbe/zmq4
RUN go get -u github.com/aws/aws-sdk-go
RUN go get -u github.com/go-redis/redis
RUN go get -u github.com/pkg/errors
RUN go get -u github.com/google/uuid
RUN go get -u github.com/montanaflynn/stats
RUN go get -u k8s.io/client-go/kubernetes
RUN go get -u k8s.io/client-go/tools/clientcmd
RUN go get -u k8s.io/apimachinery/pkg/apis/meta/v1

# For the k8s config
RUN mkdir -p /root/.kube

# Clone the AFT code.
RUN mkdir -p $GOPATH/src/github.com/hydro-project
WORKDIR $GOPATH/src/github.com/hydro-project
RUN git clone https://github.com/hydro-project/aft

# Clone the TASC code.
RUN mkdir -p $GOPATH/src/github.com/saurav-c
WORKDIR $GOPATH/src/github.com/saurav-c
RUN git clone https://github.com/saurav-c/aftsi
WORKDIR aftsi

# If file exists, delete it
RUN rm -f config/tasc-config.yml

# Produce all keynode, aftsi and routing pb.go files
WORKDIR proto
RUN mkdir -p aftsi/api
RUN mkdir -p keynode/api
RUN mkdir -p routing/api

RUN protoc -I aftsi/ aftsi/aftsi.proto --go_out=plugins=grpc:aftsi/api
RUN protoc -I keynode/ keynode/keynode.proto --go_out=plugins=grpc:keynode/api
RUN protoc -I routing/ routing/router.proto --go_out=plugins=grpc:routing/api

WORKDIR $GOPATH/src/github.com/saurav-c/aftsi/cluster
CMD bash ./init-tasc.sh