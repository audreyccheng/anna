#ifndef INCLUDE_TXN_TXN_UTILS_HPP_
#define INCLUDE_TXN_TXN_UTILS_HPP_

#include <fstream>
#include <string>
#include <unistd.h> // for ftruncate(2)

#include "base_txn_node.hpp"
#include "txn_operation.hpp"
#include "common.hpp"
#include "kvs_common.hpp"
#include "yaml-cpp/yaml.h"

// // Define the garbage collect threshold
// #define GARBAGE_COLLECT_THRESHOLD 10000000

// // Define the data redistribute threshold
// #define DATA_REDISTRIBUTE_THRESHOLD 50

// // Define the gossip period (frequency)
// #define PERIOD 10000000 // 10 seconds

typedef TxnNode<Key, vector<Operation>> BaseTxn;
typedef BaseNode<Key, string> BaseStore;

// a map that represents which keys should be sent to which IP-port combinations
// typedef map<Address, set<Key>> AddressKeysetMap;

class TxnSerializer {
public:
  virtual string get_ops(const string &txn_id, AnnaError &error) = 0;
  virtual void put_op(const string &txn_id, const string &serialized) = 0;
  virtual void put_start_txn(const string &txn_id) = 0;
  virtual void create_txn(const string &client_id) = 0;
  virtual void commit_txn(const string &txn_id) = 0;
  virtual void remove(const Key &key) = 0;
  virtual ~TxnSerializer(){};
};

class BaseTxnSerializer : public TxnSerializer {
  BaseTxn *base_txn_node_;

public:
  BaseTxnSerializer(BaseTxn *base_txn_node_) : base_txn_node_(base_txn_node) {}

  // TODO(@accheng): do we need this?
  string get_ops(const string &txn_id, AnnaError &error) {
    auto ops = base_txn_node_->get(key, error);
    return serialize(vals);
  }

  void put_op(const string &txn_id, const Key &k, const string &payload, AnnaError &error) {
    base_txn_node_->put_op(txn_id, Operation(k, payload), error);
  }

  void put_start_txn(const string &txn_id) {
    base_txn_node->put_start_txn(txn_id);
  }

  string create_txn(const string &client_id) {
    return base_txn_node_->create_txn(key);
  }

  void commit_txn(const string &txn_id, AnnaError &error) {
    base_txn_node_->commit_txn(txn_id, error);
  }
};

class BaseSerializer {
public:
  virtual string get(const Key &key, AnnaError &error) = 0;
  virtual void put(const Key &key, const string &serialized) = 0;
  virtual void remove(const Key &key) = 0;
  virtual ~BaseSerializer(){};
};

class BaseStoreSerializer : public BaseSerializer {
  BaseStore *base_node_;

public:
  BaseStoreSerializer(BaseStore *base_node_) : base_node_(base_node) {}

  string get(const Key &key, AnnaError &error) {
    auto val = base_node_->get(key, error);

    if (val == "") {
      error = AnnaError::KEY_DNE;
    }

    // return serialize(val);
    return val;
  }

  void put(const Key &key, const string &serialized) {
    // Operation val = deserialize_op(serialized);
    base_txn_node_->put(key, serialized);
    // return base_txn_node_->size(key);
  }

  void remove(const Key &key) { base_node_->remove(key); }

};

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