print_help()
{
   # Display Help
   echo "The setup script can setup TASC Nodes, Keynodes, Benchmark Nodes, the client CLI, and the router nodes on AWS EC2 instances."
   echo
   echo "Use the config file to configure the particular node."
}

while getopts 'bh' flag; do
  case "${flag}" in
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
sudo wget https://github.com/protocolbuffers/protobuf/releases/download/v3.10.0/protoc-3.10.0-linux-x86_64.zip
sudo unzip protoc-3.10.0-linux-x86_64.zip -d /usr/local
sudo go get -u google.golang.org/grpc
sudo go get -u github.com/golang/protobuf/protoc-gen-go
sudo go get -u github.com/pebbe/zmq4
sudo go get -u github.com/aws/aws-sdk-go
sudo go get -u github.com/google/uuid
sudo go get -u github.com/montanaflynn/stats

# Making the directory
sudo mkdir -p $GOPATH/src/github.com/saurav-c
cd $GOPATH/src/github.com/saurav-c

# Cloning GitHub repo
sudo git clone https://github.com/saurav-c/tasc

# Configuring based on node desired
cd tasc/proto/

# Giving Ubuntu User Write Access in the Go folder
sudo chmod 777 -R /home/ubuntu/go

protoc -I tasc/ tasc/tasc.proto --go_out=plugins=grpc:tasc/
protoc -I keynode/ keynode/keynode.proto --go_out=keynode/
protoc -I router/ router/router.proto --go_out=router/
protoc -I monitor/ monitor/monitor.proto --go_out=monitor/

if [[ "$1" = "tasc" ]]
then
  # Creating the executable for TASC
  cd $GOPATH/src/github.com/saurav-c/tasc/cmd/tasc
  sudo go build
  ./tasc
fi

if [[ "$1" = "keynode" ]]
then
  # Creating the executable for Keynode
  cd $GOPATH/src/github.com/saurav-c/tasc/cmd/keynode
  sudo go build
  ./keynode
fi

if [[ "$1" = "cli" ]]
then
  # Creating the executable for CLI
  cd $GOPATH/src/github.com/saurav-c/tasc/cli
  sudo go build
  ./cli
fi

if [[ "$1" = "routing" ]]
then
  # Creating the executable for Router
  cd $GOPATH/src/github.com/saurav-c/tasc/cmd/router
  sudo go build
  mode=$2
  ./router -mode $mode
fi

if [[ "$1" = "benchmark" ]]
then
  # Creating the executable for Router
  cd $GOPATH/src/github.com/saurav-c/tasc/cmd/benchmark
  sudo go build
  ./benchmark -address $2 -type $3 -numReq $4 -numThreads $5 -rtr $6
fi
