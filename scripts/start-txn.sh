cp conf/anna-txn.yml conf/anna-config.yml

trap 'kill %1; kill %2; kill %3' SIGINT

./build/target/kvs/anna-monitor &
MPID=$!
./build/target/kvs/anna-route &
RPID=$!

export SERIALIZABILITY_PROTOCOL="mvcc"
export SERVER_TYPE="txn"
./build/target/kvs/anna-txn &
S2PID=$!

echo $MPID
echo $RPID
echo $S2PID

./build/cli/anna-txn-cli conf/anna-txn.yml
