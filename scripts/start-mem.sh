cp conf/anna-mem.yml conf/anna-config.yml

export SERIALIZABILITY_PROTOCOL="mvcc"
export SERVER_TYPE="memory"
./build/target/kvs/anna-txn
