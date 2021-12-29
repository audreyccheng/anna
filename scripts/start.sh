TYPE=$1
PROTOCOL=$2

export SERVER_TYPE="$TYPE"
export SERIALIZABILITY_PROTOCOL="$PROTOCOL"

if [ "$TYPE" = "txn" ]; then
  trap 'kill %1; kill %2; kill %3' SIGINT

  ./build/target/kvs/anna-monitor &
  ./build/target/kvs/anna-route &
  ./build/target/kvs/anna-txn &
  ./build/cli/anna-txn-cli conf/anna-config.yml
elif [ "$TYPE" = "memory" ] || [ "$TYPE" = "ebs" ] || [[ "$TYPE" = "log" ]] ; then
  ./build/target/kvs/anna-txn
elif [ "$TYPE" = "txn-bench" ]; then
  trap 'kill %1; kill %2; kill %3; kill %4' SIGINT

  ./build/target/kvs/anna-monitor &
  ./build/target/kvs/anna-route &

  export SERVER_TYPE="txn"
  ./build/target/kvs/anna-txn &
  ./build/target/benchmark/anna-txn-bench &
  ./build/target/benchmark/anna-bench-trigger 1
else
  echo "Usage: start.sh [txn|memory|log|txn-bench] [mvcc|locking]"
fi