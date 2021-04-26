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
  virtual unsigned size() = 0;
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

  unsigned size() { return base_node_->size(); }

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

  void commit(const string& txn_id, const Key &key, AnnaError &error) { // TODO(@accheng): what to do with error?
    lock_node_->release_rlock(txn_id, key);
    lock_node_->release_wlock(txn_id, key);
  }

  unsigned size() { return lock_node_->size(); }

  void remove(const string& txn_id, const Key &key) { lock_node_->remove(key); }

};

class DiskLockStoreSerializer : public BaseSerializer {
  unsigned tid_;
  string ebs_root_;
  LockStore *lock_node_;

public:
  DiskLockStoreSerializer(unsigned &tid,
                          LockStore *lock_node) : tid_(tid), lock_node_(lock_node) {
    // TODO(@accheng): should locks and values be kept in memory?
    YAML::Node conf = YAML::LoadFile("conf/anna-config.yml");

    ebs_root_ = conf["ebs"].as<string>();

    if (ebs_root_.back() != '/') {
      ebs_root_ += "/";
    }
  }

  string get(const string& txn_id, const Key &key, AnnaError &error) {
    string value;

    // open a new filestream for reading in a binary
    // check if this value has been modified by this txn
    string tfname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + txn_id + "/" + key; // TODO(@accheng): should we have multiversions?
    std::fstream tinput(tfname, std::ios::in | std::ios::binary);

    if (!tinput) {
      // try to read from current value
      string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + key;
      std::fstream input(tfname, std::ios::in | std::ios::binary);
      if (!input) {
        error = AnnaError::KEY_DNE;
      } else {
        getline(input, value); // TODO(@accheng): should only ever be 1 line?
        return value;
      }
    // TODO(@accheng): do we need this?
    // } else if (!value.ParseFromIstream(&input)) {
    //   std::cerr << "Failed to parse payload." << std::endl;
    //   error = AnnaError::KEY_DNE;
    } else {
      getline(tinput, value);
      if (value == "") {
        error = AnnaError::KEY_DNE;
      }
    }
    return value;
  }

  void put(const string& txn_id, const Key &key, const string &serialized,
           AnnaError &error) {
    // LWWValue input_value;
    // input_value.ParseFromString(serialized);

    string original_value;

    string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + txn_id + "/" + key;
    std::fstream input(fname, std::ios::in | std::ios::binary);

    // if (!input) { // in this case, this key has never been seen before, so we
                  // attempt to create a new file for it

    // ios::trunc means that we overwrite the existing file
    std::fstream output(fname,
                        std::ios::out | std::ios::trunc | std::ios::binary);

    output << serialized;
      // if (!serialized.SerializeToOstream(&output)) {
      //   std::cerr << "Failed to write payload." << std::endl;
      // }
      // return output.tellp();
    // } else if (!original_value.ParseFromIstream(
    //                &input)) { // if we have seen the key before, attempt to
    //                           // parse what was there before
    //   std::cerr << "Failed to parse payload." << std::endl;
    //   return 0;
    // } else {
    //   // if (input_value.timestamp() >= original_value.timestamp()) {
    //     std::fstream output(fname,
    //                         std::ios::out | std::ios::trunc | std::ios::binary);
    //     output << serialized;
    //     // if (!serialized.SerializeToOstream(&output)) {
    //     //   std::cerr << "Failed to write payload" << std::endl;
    //     // }
    //     // return output.tellp();
    //   } else {
    //     // return input.tellp();
    //   }
    // }
  }

  void prepare(const string& txn_id, const Key &key, AnnaError &error) {
    // nothing needs to be done?
  }

  void commit(const string& txn_id, const Key &key, AnnaError &error) {
    // TODO(@accheng): update
    lock_node_->release_rlock(txn_id, key);
    lock_node_->release_wlock(txn_id, key);
    
    // check if value was updated for this transaction
    string value;
    string tfname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + txn_id + "/" + key;
    std::fstream tinput(tfname, std::ios::in | std::ios::binary);
    if (!tinput) {
      return; // nothing needs to be done if this was only a read operation
    } else {
      getline(tinput, value);
      std::remove(tfname.c_str());
    }

    if (value != "") {
      string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + key;
      // ios::trunc means that we overwrite the existing file
      std::fstream output(fname,
                          std::ios::out | std::ios::trunc | std::ios::binary);
      output << value;
    }
  }

  void remove(const string& txn_id, const Key &key) {
    string fname = ebs_root_ + "ebs_" + std::to_string(tid_) + "/" + txn_id + "/" + key;

    if (std::remove(fname.c_str()) != 0) {
      std::cerr << "Error deleting file" << std::endl;
    }
  }
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

struct PendingTxnGossip {
  PendingTxnGossip() {}
  PendingTxnGossip(string payload)
      : payload_(std::move(payload)) {}
  string payload_;
};

#endif // INCLUDE_TXN_TXN_UTILS_HPP_