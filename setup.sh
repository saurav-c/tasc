mkdir $GOPATH
mkdir $GOPATH/bin
mkdir $GOPATH/src
mkdir $GOPATH/pkg
apt-get update
apt-get install -y software-properties-common
add-apt-repository -y ppa:longsleep/golang-backports
apt-get update
apt-get install -y golang-go wget unzip git ca-certificates net-tools python3-pip libzmq3-dev curl apt-transport-https
wget https://github.com/protocolbuffers/protobuf/releases/download/v3.10.0/protoc-3.10.0-linux-x86_64.zip
unzip protoc-3.10.0-linux-x86_64.zip -d /usr/local
mkdir -p $GOPATH/src/github.com/saurav-c
cd $GOPATH/src/github.com/saurav-c
git clone https://github.com/saurav-c/aftsi
go get -u google.golang.org/grpc
go get -u github.com/golang/protobuf/protoc-gen-go
export PATH=$PATH:$GOPATH/bin
cd proto/aftsi
protoc -I aftsi/ aftsi/aftsi.proto --go_out=plugins=grpc:aftsi
cd ../keynode
protoc -I keynode/ keynode/keynode.proto --go_out=keynode

