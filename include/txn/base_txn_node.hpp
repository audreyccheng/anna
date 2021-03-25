#ifndef INCLUDE_TXN_BASE_TXN_NODE_HPP_
#define INCLUDE_TXN_BASE_TXN_NODE_HPP_

#include "anna.pb.h"

template <typename K, typename V> class TxnNode {
protected:
  map<K, vector<V>> db;

public:
  TxnNode<K, vector<V>>() {}

  TxnNode<K, vector<V>>(map<K, vector<V>> &other) { db = other; }

  V get(const K &k, AnnaError &error) {
    if (db.find(k) == db.end()) {
      error = AnnaError::TXN_DNE;
    }

    return db.at(k);
  }

  void put(const K &k, const V &v) { 
    bool contains_k = (db.find(k) != db.end());
    // only update txn if it doesn't yet exist
    if (!contains_k) {
      vector<V> vec;
      vec.push_back(v)
      db[k] = vec;
    } else {
      db.at(k).push_back(v);
    }
    return contains_k;
  }

  void create_txn(const K &k) {
    bool contains_k = (db.find(k) != db.end());
    if (!contains_k) {
      vector<V> vec;
      db[k] = vec;
    }
    return contains_k;
  }

  unsigned size(const K &k) { return db.at(k).size(); }

  // unsigned size(const K &k) { return db.at(k).size().reveal(); }

  void remove(const K &k) { 
    if (db.find(k) != db.end()) {
      db.erase(k); 
    }
  }
};

#endif // INCLUDE_TXN_BASE_TXN_NODE_HPP_