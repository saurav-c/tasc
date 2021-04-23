if [ -z "$1" ] && [ -z "$2" ]; then
  echo "Usage: ./$0 build start-cli"
  echo ""
  echo "You must run this from the project root directory."
  exit 1
fi

if [ "$1" = "y" ] || [ "$1" = "yes" ]; then
  echo "Compiling TASC..."
  ./scripts/build-local.sh
fi

./cmd/manager/manager &
TXID=$!
echo "Started Transaction Manager"
./cmd/keynode/keynode &
KID=$!
echo "Started Key Node"
./cmd/worker/worker &
WID=$!
echo "Started Worker"
./cmd/monitor/monitor &
MID=$!
echo "Started Monitoring Node"

echo $TXID > pids
echo $KID >> pids
echo $WID >> pids
echo $MID >> pids

if [ "$2" = "y" ] || [ "$2" = "yes" ]; then
  sleep 2 # wait for gRPC servers to start
  ./cmd/cli/cli -local
fi