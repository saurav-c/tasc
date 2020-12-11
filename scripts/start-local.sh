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

./cmd/aftsi/aftsi &
TXID=$!
echo "Started Transaction Manager"
./cmd/keynode/keynode &
KID=$!
echo "Started Key Node"
./cmd/routing/routing -mode key &
RID=$!
echo "Started Key Router"

echo $TXID > pids
echo $KID >> pids
echo $RID >> pids

if [ "$2" = "y" ] || [ "$2" = "yes" ]; then
  sleep 2 # wait for gRPC servers to start
  ./cli/cli
fi