# Save current working directory
CWD=`pwd`

cd ./cmd/aftsi
go build
echo "Built Transaction Manager"

cd ../keynode
go build
echo "Built Key Node"

cd ../routing
go build
echo "Built Key Router"

cd ../monitor
go build
echo "Built Monitoring Node"

cd ../../cli
go build

cd $CWD