trap 'kill %1; kill %2; kill %3; kill %4' SIGINT

./build/target/kvs/anna-monitor &
./build/target/kvs/anna-route &

export SERIALIZABILITY_PROTOCOL="mvcc"
export SERVER_TYPE="txn"
./build/target/kvs/anna-txn &

./build/target/benchmark/anna-txn-bench &

./build/target/benchmark/anna-bench-trigger 1