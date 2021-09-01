trap 'kill %1; kill %2; kill %3' SIGINT

./build/target/kvs/anna-monitor &
./build/target/kvs/anna-route &

export SERIALIZABILITY_PROTOCOL="mvcc"
export SERVER_TYPE="txn"
./build/target/kvs/anna-txn &

./build/cli/anna-txn-cli conf/anna-config.yml
