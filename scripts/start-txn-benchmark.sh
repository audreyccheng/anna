cp conf/anna-txn.yml conf/anna-config.yml

./build/target/kvs/anna-monitor &
MPID=$!
./build/target/kvs/anna-route &
RPID=$!

export SERIALIZABILITY_PROTOCOL="locking"
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