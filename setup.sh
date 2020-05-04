batch_mode=false

print_help()
{
   # Display Help
   echo "The setup script can setup AFTSI Nodes, Keynodes, Benchmark Nodes, the AFTSI CLI, and the router nodes on AWS EC2 instances."
   echo
   echo "To use batch mode, use the flag -b."
   echo "Syntax for AFTSI: ./aftsi -addr \$0 -txnRtr \$1 -keyrtr \$2 -storage \$3 -batch batch_mode"
   echo "Syntax for Keynode: ./keynode -storage \$0 -batch batch_mode"
   echo "Syntax for Routing Node: ./routing \$*"
   echo "Syntax for Benchmark Node: ./benchmark -address \$0 -type \$1 -numReq \$2 -numThreads \$3 -rtr \$4"
   echo "Syntax for CLI: ./cli \$0"
}


while getopts 'bh' flag; do
  case "${flag}" in
    b) batch_mode=true ;;
    h)  print_help; exit 1;;
  esac
done
shift $((OPTIND -1))

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
sudo go get -u github.com/google/uuid
sudo go get -u github.com/montanaflynn/stats

# Making the directory
sudo mkdir -p $GOPATH/src/github.com/saurav-c
cd $GOPATH/src/github.com/saurav-c

# Cloning GitHub repo
sudo git clone https://github.com/saurav-c/aftsi

# Configuring based on node desired
cd aftsi/proto/

# Giving Ubuntu User Write Access
sudo chmod 777 -R /home/ubuntu

protoc -I aftsi/ aftsi/aftsi.proto --go_out=plugins=grpc:aftsi
sudo mkdir -p aftsi/api
sudo mv aftsi/aftsi.pb.go aftsi/api

protoc -I keynode/ keynode/keynode.proto --go_out=plugins=grpc:keynode
sudo mkdir -p keynode/api
sudo mv keynode/keynode.pb.go keynode/api

protoc -I routing/ routing/router.proto --go_out=plugins=grpc:routing
sudo mkdir -p routing/api
sudo mv routing/router.pb.go routing/api

if [[ "$0" = "aftsi" ]]
then
  # Creating the executable for AFTSI
  cd $GOPATH/src/github.com/saurav-c/aftsi/cmd/aftsi
  sudo go build
  ./aftsi -addr $1 -txnrtr $2 -keyrtr $3 -storage $4 -batch batch_mode
fi

if [[ "$0" = "keynode" ]]
then
  # Creating the executable for Keynode
  cd $GOPATH/src/github.com/saurav-c/aftsi/cmd/keynode
  sudo go build
  ./keynode -storage $1 -batch batch_mode
fi

if [[ "$0" = "cli" ]]
then
  # Creating the executable for CLI
  cd $GOPATH/src/github.com/saurav-c/aftsi/cli
  sudo go build
  ./cli $1
fi

if [[ "$0" = "routing" ]]
then
  # Creating the executable for Router
  cd $GOPATH/src/github.com/saurav-c/aftsi/cmd/routing
  sudo go build
  mode=$1
  shift 2
  ./routing -mode $mode $*
fi

if [[ "$0" = "benchmark" ]]
then
  # Creating the executable for Router
  cd $GOPATH/src/github.com/saurav-c/aftsi/benchmark
  sudo go build
  ./benchmark -address $1 -type $2 -numReq $3 -numThreads $4 -rtr $5
fi






