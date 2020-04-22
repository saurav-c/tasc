export GOPATH=~/go
echo "export GOPATH=~/go" >> ~/.bashrc
mkdir $GOPATH
mkdir $GOPATH/bin
mkdir $GOPATH/src
mkdir $GOPATH/pkg
echo "PATH=$PATH:$GOPATH/bin" >> ~/.bashrc
export PATH=$PATH:$GOPATH/bin
sudo apt-get update
sudo apt-get install -y software-properties-common
sudo add-apt-repository -y ppa:longsleep/golang-backports
sudo apt-get update
sudo apt-get install -y golang-go wget unzip git ca-certificates net-tools python3-pip libzmq3-dev curl apt-transport-https
wget https://github.com/protocolbuffers/protobuf/releases/download/v3.10.0/protoc-3.10.0-linux-x86_64.zip
sudo unzip protoc-3.10.0-linux-x86_64.zip -d /usr/local
mkdir -p $GOPATH/src/github.com/saurav-c
cd $GOPATH/src/github.com/saurav-c
sudo git clone https://github.com/saurav-c/aftsi
cd afsti
sudo go get -u google.golang.org/grpc
sudo go get -u github.com/golang/protobuf/protoc-gen-go

cd proto/aftsi
sudo protoc -I aftsi/ aftsi/aftsi.proto --go_out=plugins=grpc:aftsi
cd $GOPATH/src/github.com/saurav-c/aftsi/cmd/aftsi
sudo go build
sudo ./aftsi

