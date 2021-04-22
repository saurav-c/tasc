# Scripts to run to make EC2 ready for Lambda Generation

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

sudo yum install -y golang git wget unzip python3 curl unzip ca-certificates net-tools curl zeromq python3-devel gcc-c++

# Install protobuf
sudo wget https://github.com/protocolbuffers/protobuf/releases/download/v3.10.0/protoc-3.10.0-linux-x86_64.zip
sudo unzip protoc-3.10.0-linux-x86_64.zip -d /usr/local\

# Making the directory
sudo mkdir -p $GOPATH/src/github.com/saurav-c
cd $GOPATH/src/github.com/saurav-c

# Cloning GitHub repo
sudo git clone https://github.com/saurav-c/tasc

# Entering the tasc repo
cd tasc/proto

# Giving User Write Access in the Go folder
sudo chmod 777 -R /home/ec2-user/go

# Installing all Python packages
sudo python3 -m pip install grpcio
sudo python3 -m pip install grpcio-tools

python3 -m grpc_tools.protoc -I tasc/ tasc/tasc.proto --python_out=tasc/ --grpc_python_out=tasc/

# Generating the folder for the TASC Lambda (w/ all the dependencies)
mkdir -p ~/tasc-lambda
mv tasc/tasc_pb2.py ~/tasc-lambda
mv tasc/tasc_pb2_grpc.py ~/tasc-lambda

pip3 install --target ~/tasc-lambda grpcio protobuf numpy scipy pyzmq





