cp conf/anna-log.yml conf/anna-config.yml

export SERIALIZABILITY_PROTOCOL="mvcc"
export SERVER_TYPE="log"
./build/target/kvs/anna-log &
S2PID=$!

echo $S2PID

./build/cli/anna-txn-cli conf/anna-log.yml
