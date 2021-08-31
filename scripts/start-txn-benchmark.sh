cp conf/anna-txn.yml conf/anna-config.yml

trap 'kill %1; kill %2; kill %3; kill %4' SIGINT

./build/target/kvs/anna-monitor &
MPID=$!
./build/target/kvs/anna-route &
RPID=$!

export SERIALIZABILITY_PROTOCOL="mvcc"
export SERVER_TYPE="txn"
./build/target/kvs/anna-txn &
S2PID=$!

./build/target/benchmark/anna-txn-bench &
BPID=$!

echo $MPID
echo $RPID
echo $S2PID
echo $BPID

./build/target/benchmark/anna-bench-trigger 1