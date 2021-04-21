#ifndef INCLUDE_TXN_TXN_UTILS_HPP_
#define INCLUDE_TXN_TXN_UTILS_HPP_

#include <fstream>
#include <string>
#include <unistd.h> // for ftruncate(2)

#include "base_txn_node.hpp"
#include "base_store.hpp"
#include "base_log.hpp"
#include "common.hpp"
#include "kvs_common.hpp"
#include "yaml-cpp/yaml.h"

// // Define the garbage collect threshold
// #define GARBAGE_COLLECT_THRESHOLD 10000000

// // Define the data redistribute threshold
// #define DATA_REDISTRIBUTE_THRESHOLD 50

// // Define the gossip period (frequency)
// #define PERIOD 10000000 // 10 seconds

typedef TxnNode<Key> BaseTxn;
typedef BaseNode<Key, string> BaseStore;
typedef LockNode<Key> LockStore;
// typedef LogNode<Operation> BaseLog;

// a map that represents which keys should be sent to which IP-port combinations
// typedef map<Address, set<Key>> AddressKeysetMap;

class TxnSerializer {
public:
  virtual vector<Operation> get_ops(const string &txn_id, AnnaError &error) = 0;
  virtual string get_ops_serialized(const string &txn_id, AnnaError &error) = 0;
  virtual void put_op(const string &txn_id, const Key &k,
                      const string &payload, AnnaError &error) = 0;
  virtual void put_start_txn(const string &txn_id) = 0;
  virtual string create_txn(const string &client_id) = 0;
  virtual void commit_txn(const string &txn_id, AnnaError &error) = 0;
  // virtual void remove(const Key &key) = 0;
  virtual ~TxnSerializer(){};
};

class BaseTxnSerializer : public TxnSerializer {
  BaseTxn *base_txn_node_;

public:
  BaseTxnSerializer(BaseTxn *base_txn_node) : base_txn_node_(base_txn_node) {}

  vector<Operation> get_ops(const string &txn_id, AnnaError &error) {
    return base_txn_node_->get_ops(txn_id, error);
  }

  // TODO(@accheng): do we need this?
  string get_ops_serialized(const string &txn_id, AnnaError &error) {
    vector<Operation> ops = base_txn_node_->get_ops(txn_id, error);
    return serialize(ops);
  }

  void put_op(const string &txn_id, const Key &k,
              const string &payload, AnnaError &error) {
    auto new_op = Operation(txn_id, k, payload);
    base_txn_node_->put_op(txn_id, new_op, error);
  }

  void put_start_txn(const string &txn_id) {
    base_txn_node_->put_start_txn(txn_id);
  }

  string create_txn(const string &client_id) {
    return base_txn_node_->create_txn(client_id);
  }

  void commit_txn(const string &txn_id, AnnaError &error) {
    base_txn_node_->commit_txn(txn_id, error);
  }
};

class BaseSerializer {
public:
  virtual string get(const string& txn_id, const Key &key,
                     AnnaError &error) = 0;
  virtual void put(const string& txn_id, const Key &key,
                   const string &serialized, AnnaError &error) = 0;
  virtual void prepare(const string& txn_id, const Key &key,
                       AnnaError &error) = 0;
  virtual void commit(const string& txn_id, const Key &key,
                      AnnaError &error) = 0;
  virtual void remove(const string& txn_id, const Key &key) = 0;
  virtual ~BaseSerializer(){};
};

class BaseStoreSerializer : public BaseSerializer {
  BaseStore *base_node_;

public:
  BaseStoreSerializer(BaseStore *base_node) : base_node_(base_node) {}

  string get(const string& txn_id, const Key &key, AnnaError &error) {
    auto val = base_node_->get(key, error);

    if (val == "") {
      error = AnnaError::KEY_DNE;
    }

    // return serialize(val);
    return val;
  }

  void put(const string& txn_id, const Key &key,
           const string &serialized, AnnaError &error) {
    // Operation val = deserialize_op(serialized);
    base_node_->put(key, serialized);
    // return base_txn_node_->size(key);
  }

  void prepare(const string& txn_id, const Key &key, AnnaError &error) {
    // nothing needs to be done
  }

  void commit(const string& txn_id, const Key &key, AnnaError &error) {
    // nothing needs to be done
  }

  void remove(const string& txn_id, const Key &key) { base_node_->remove(key); }

};

class LockStoreSerializer : public BaseSerializer {
  LockStore *lock_node_;

public:
  LockStoreSerializer(LockStore *lock_node) : lock_node_(lock_node) {}

  string get(const string& txn_id, const Key &key, AnnaError &error) {
    auto val = lock_node_->get(txn_id, key, error);

    // return serialize(val);
    return val;
  }

  void put(const string& txn_id, const Key &key, const string &serialized,
           AnnaError &error) {
    // Operation val = deserialize_op(serialized);
    lock_node_->put(txn_id, key, serialized, error);
    // return base_txn_node_->size(key);
  }

  void prepare(const string& txn_id, const Key &key, AnnaError &error) {
    // nothing needs to be done?
  }

  void commit(const string& txn_id, const Key &key, AnnaError &error) {
    lock_node_->release_rlock(txn_id, key);
    lock_node_->release_wlock(txn_id, key);
  }

  void remove(const string& txn_id, const Key &key) { lock_node_->remove(key); }

};

class LogSerializer {
public:
  virtual unsigned append(const string &serialized) = 0;
  virtual unsigned append(const string &txn_id, const Key &key,
                          const string &payload) = 0;
  virtual void trim(const unsigned &l) = 0;
  virtual string read(const unsigned &l, AnnaError &error) = 0;
  virtual string subscribe(const unsigned &l, AnnaError &error) = 0;
  virtual unsigned size() = 0;
  virtual ~LogSerializer(){};
};

class BaseLogSerializer : public LogSerializer {
  BaseLog *base_log_node_;

public:
  BaseLogSerializer(BaseLog *base_log_node) : base_log_node_(base_log_node) {}

  unsigned append(const string &serialized) {
    Operation op = deserialize_op(serialized);
    return base_log_node_->append(op);
  }

  unsigned append(const string &txn_id, const Key &key,
                  const string &payload) {
    return base_log_node_->append(Operation(txn_id, key, payload));
  }

  void trim(const unsigned &l) {
    base_log_node_->trim(l);
  }

  string read(const unsigned &l, AnnaError &error) {
    auto val = base_log_node_->read(l, error);
    return serialize(val);
  }

  string subscribe(const unsigned &l, AnnaError &error) {
    auto vals = base_log_node_->subscribe(l, error);
    return serialize(vals);
  }

  unsigned size() {
    return base_log_node_->size();
  }
};

using RequestTypeMap =
    std::unordered_map<RequestType, vector<unsigned>, request_type_hash>;

// using SerializerMap =
//     std::unordered_map<LatticeType, Serializer *, lattice_type_hash>;

struct PendingTxnRequest {
  PendingTxnRequest() {}
  PendingTxnRequest(RequestType type, string txn_id, Key key,
                    string payload, Address addr, string response_id)
      : type_(type), txn_id_(std::move(txn_id)), key_(std::move(key)),
        payload_(std::move(payload)), addr_(addr), response_id_(response_id) {}

  RequestType type_;
  string txn_id_;
  Key key_;
  string payload_;
  Address addr_;
  string response_id_;
};

// struct PendingGossip {
//   PendingGossip() {}
//   PendingGossip(LatticeType lattice_type, string payload)
//       : lattice_type_(std::move(lattice_type)), payload_(std::move(payload)) {}
//   LatticeType lattice_type_;
//   string payload_;
// };

#endif // INCLUDE_TXN_TXN_UTILS_HPP_