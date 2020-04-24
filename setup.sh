# Creating the Directories and Defining ENVVARS
export GOPATH=~/go
sudo mkdir $GOPATH
sudo mkdir $GOPATH/bin
sudo mkdir $GOPATH/src
sudo mkdir $GOPATH/pkg
export PATH=$PATH:$GOPATH/bin
export GOBIN=$GOPATH/bin

# Persist Information in ~/.bashrc
echo "export GOPATH=~/go" >> ~/.bashrc
echo "PATH=$PATH:$GOPATH/bin" >> ~/.bashrc
echo "export GOBIN=$GOPATH/bin" >> ~/.bashrc

# Installing the dependencies
sudo apt-get update
sudo apt-get install -y software-properties-common
sudo add-apt-repository -y ppa:longsleep/golang-backports
sudo apt-get update
sudo apt-get install -y golang-go wget unzip git ca-certificates net-tools python3-pip libzmq3-dev curl apt-transport-https
wget https://github.com/protocolbuffers/protobuf/releases/download/v3.10.0/protoc-3.10.0-linux-x86_64.zip
sudo unzip protoc-3.10.0-linux-x86_64.zip -d /usr/local
sudo go get -u google.golang.org/grpc
sudo go get -u github.com/golang/protobuf/protoc-gen-go
sudo go get -u github.com/pebbe/zmq4
sudo go get -u github.com/aws/aws-sdk-go
sudo go get -u github.com/go-redis/redis
sudo go get -u github.com/pkg/errors

# Making the directory
sudo mkdir -p $GOPATH/src/github.com/saurav-c
cd $GOPATH/src/github.com/saurav-c

# Cloning GitHub repo
sudo git clone https://github.com/saurav-c/aftsi

# Configuring based on node desired
cd aftsi/proto/
if [[ "$1" = "aftsi" ]]
then
  sudo protoc -I aftsi/ aftsi/aftsi.proto --go_out=plugins=grpc:aftsi
  sudo mkdir -p aftsi/api
  sudo mv aftsi/aftsi.pb.go aftsi/api

  # Creating the executable for AFTSI
  cd $GOPATH/src/github.com/saurav-c/aftsi/cmd/aftsi
  sudo go build
  ./aftsi $2 $3 $4
fi

if [[ "$1" = "keynode" ]]
then
  sudo protoc -I keynode/ keynode/keynode.proto --go_out=plugins=grpc:keynode
  sudo mkdir -p keynode/api
  sudo mv keynode/keynode.pb.go keynode/api

  # Creating the executable for AFTSI
  cd $GOPATH/src/github.com/saurav-c/aftsi/cmd/keynode
  sudo go build
  ./keynode
fi

if [[ "$1" = "cli" ]]
then
  # Creating the executable for AFTSI
  cd $GOPATH/src/github.com/saurav-c/aftsi/cli
  sudo go build
  ./cli $2
fi






