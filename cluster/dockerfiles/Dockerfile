FROM ubuntu:18.04
MAINTAINER Saurav Chhatrapati <sauravc@berkeley.edu> version: 1.0

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
ENV TASC_HOME $GOPATH/src/github.com/saurav-c/tasc

# Go ENV Variable
ENV GO111MODULE auto

# Install Go, other Ubuntu dependencies.
RUN apt-get update
RUN apt-get install -y software-properties-common
RUN add-apt-repository -y ppa:longsleep/golang-backports
RUN apt-get update
RUN apt-get install -y golang-go wget unzip git ca-certificates net-tools python3-pip libzmq3-dev dnsutils curl apt-transport-https

# Install kubectl for the management pod
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
RUN go get -u github.com/pebbe/zmq4
RUN go get -u github.com/aws/aws-sdk-go
RUN go get -u github.com/google/uuid
RUN go get -u github.com/nu7hatch/gouuid
RUN go get -u github.com/montanaflynn/stats
RUN go get -u k8s.io/client-go/kubernetes
RUN go get -u k8s.io/client-go/tools/clientcmd
RUN go get -u k8s.io/apimachinery/pkg/apis/meta/v1
RUN go get -u github.com/sirupsen/logrus
RUN go get -u github.com/golang/protobuf/protoc-gen-go

# Install required Python dependencies.
RUN pip3 install zmq kubernetes boto3 numpy

# Clone the TASC code.
RUN mkdir -p $GOPATH/src/github.com/saurav-c
WORKDIR $GOPATH/src/github.com/saurav-c
RUN git clone https://github.com/saurav-c/tasc
WORKDIR tasc

# Calls for a random number to break the cahing of the git clone
# (https://stackoverflow.com/questions/35134713/disable-cache-for-specific-run-commands/58801213#58801213)
ADD "https://www.random.org/cgi-bin/randbyte?nbytes=10&format=h" skipcache
RUN git pull

WORKDIR $GOPATH/src/github.com/saurav-c/tasc/cluster/dockerfiles
CMD bash ./init-tasc.sh
