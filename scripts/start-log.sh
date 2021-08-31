cp conf/anna-log.yml conf/anna-config.yml

export SERIALIZABILITY_PROTOCOL="mvcc"
export SERVER_TYPE="log"
./build/target/kvs/anna-txn
