#ifndef INCLUDE_TXN_BASE_TXN_NODE_HPP_
#define INCLUDE_TXN_BASE_TXN_NODE_HPP_

#include "anna.pb.h"
#include "common.hpp"

// template <typename K> 
class TxnNode {
protected:
  map<Key, vector<Operation>> db;

public:
  TxnNode() {}
  // TxnNode<K, vector<Operation>>() {}

  // TxnNode<K, vector<Operation>>(map<K, vector<Operation>> &other) { 
  TxnNode(map<Key, vector<Operation>> &other) { 
    db = other; 
  }

  vector<Operation> get_ops(const string &txn_id, AnnaError &error) {
    if (db.find(txn_id) == db.end()) {
      error = AnnaError::TXN_DNE;
      vector<Operation> vec;
      return vec;
    }
    
    return db.at(txn_id);
  }

  void put_op(const string &txn_id, const Operation &v, AnnaError &error) { 
    if (db.find(txn_id) == db.end()) {
      error = AnnaError::TXN_DNE;
    } else {
      db.at(txn_id).push_back(v);
    }
  }

  string create_txn(const string &client_id) {
    // TODO(@accheng): should use time in txn_id?
    string time = std::to_string(
      std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    string txn_id = client_id + "-" + std::to_string(rand()) + ":" + time;
    vector<Operation> vec;
    db[txn_id] = vec;
    return txn_id;
  }

  void put_start_txn(const string &txn_id) {
    vector<Operation> vec;
    db[txn_id] = vec;
  }

  unsigned size(const string &txn_id) {
    if (db.find(txn_id) == db.end()) {
      return 0;
    }
    return db.at(txn_id).size(); 
  }

  void commit_txn(const string &txn_id, AnnaError &error) {
    if (db.find(txn_id) == db.end()) {
      error = AnnaError::TXN_DNE;
    } else {
      // TODO(@accheng): update
      db.erase(txn_id);
    } 
  }

};

#endif // INCLUDE_TXN_BASE_TXN_NODE_HPP_