#ifndef INCLUDE_TXN_BASE_TXN_NODE_HPP_
#define INCLUDE_TXN_BASE_TXN_NODE_HPP_

#include "anna.pb.h"

template <typename K, typename V> class TxnNode {
protected:
  map<K, vector<V>> db;

public:
  TxnNode<K, vector<V>>() {}

  TxnNode<K, vector<V>>(map<K, vector<V>> &other) { db = other; }

  vector<V> get_ops(const string &txn_id, AnnaError &error) {
    if (db.find(txn_id) == db.end()) {
      error = AnnaError::TXN_DNE;
    } else {
      return db.at(txn_id)
    }
    vector<V> vec;
    return vec;
  }

  void put_op(const string &txn_id, const V &v, AnnaError &error) { 
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
    string txn_id = client_id + ":" + time;
    vector<V> vec;
    db[txn_id] = vec;
    return txn_id;
  }

  void put_start_txn(const string &txn_id) {
    vector<V> vec;
    db[txn_id] = vec;
  }

  unsigned size(const string &txn_id) { return db.at(txn_id).size(); }

  void commit_txn(const string &txn_id, AnnaError &error) {
    if (db.find(txn_id) == db.end()) {
      error = AnnaError::TXN_DNE;
    } else {
      db.erase(txn_id);
    } 
  }

};

#endif // INCLUDE_TXN_BASE_TXN_NODE_HPP_