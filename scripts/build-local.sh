# Save current working directory
CWD=`pwd`

cd ./cmd/manager
go build
echo "Built Transaction Manager"

cd ../keynode
go build
echo "Built Key Node"

cd ../worker
go build
echo "Built Worker"

cd ../monitor
go build
echo "Built Monitoring Node"

cd ../cli
go build

cd $CWD