#ifndef INCLUDE_TXN_BASE_STORE_HPP_
#define INCLUDE_TXN_BASE_STORE_HPP_

#include "anna.pb.h"
#include "types.hpp"

// Basic class for storage node using map<key, value>
template <typename K, typename V> class BaseNode {
protected:
  map<K, V> db;
  // virtual void get(const K &k, AnnaError &error) = 0;
  // virtual void put(const K &k, const V &v) = 0;

public:
  BaseNode<K, V>() {}

  BaseNode<K, V>(map<K, V> &other) { db = other; }

  V get(const K &k, AnnaError &error) {
    if (db.find(k) == db.end()) {
      error = AnnaError::KEY_DNE;
    }

    return db.at(k);
  }

  void put(const K &k, const V &v) { db[k] = v; }

  unsigned size() { return db.size(); }

  void remove(const K &k) { db.erase(k); }
};

// template <typename T>
class LockElement {
 protected:
  string element;
  string temp_element;
  // TODO(@accheng): should this be a vector?
  set<string> rlocks;
  string wlock;

 public:
  LockElement() {}

  LockElement(const string &e) { assign(e); }

  LockElement(const LockElement &other) { assign(other.reveal()); }

  // virtual ~LockElement() = default;
  LockElement &operator=(const LockElement &rhs) {
    assign(rhs.reveal());
    return *this;
  }

  bool operator==(const LockElement &rhs) const {
    return this->reveal() == rhs.reveal();
  }

  const string &reveal() const { return element; }

  void assign(const string e) { element = e; }

  void assign(const LockElement &e) { element = e.reveal(); }

  bool acquire_rlock(const string& txn_id) {
    if (wlock == "") {
      rlocks.insert(txn_id);
      return true;
    }
    return false;
  }

  bool acquire_wlock(const string& txn_id) {
    if (rlocks.empty() && wlock == "") {
      wlock = txn_id;
      return true;
    }
    return false;
  }

  void update_temp_value(const string& value) {
    temp_element = value;
  }

  void update_value() {
    element = temp_element;
  }

  // TODO(@accheng): return error if not holding read lock?
  void release_rlock(const string& txn_id) {
    auto it = rlocks.find(txn_id);
    if (it != rlocks.end()) {
      rlocks.erase(it);
    }
  }

  void release_wlock(const string& txn_id) {
    wlock = "";
  }

  bool holds_rlock(const string& txn_id) {
    return (rlocks.find(txn_id) != rlocks.end());
  }

  bool holds_wlock(const string& txn_id) {
    return wlock == txn_id;
  }
};

template <typename K> class LockNode {
protected:
  map<K, LockElement> db;

public:
  LockNode<K>() {}

  LockNode<K>(map<K, LockElement> &other) { db = other; }

  string get(const string& txn_id, const K &k, AnnaError &error) {
    if (db.find(k) == db.end()) {
      error = AnnaError::KEY_DNE;
      return ""; 
    } 
    
    // TODO(@accheng): return some value even if no rlock?
    if (!db.at(k).acquire_rlock(txn_id)) {
      error = AnnaError::FAILED_OP;
    }
    return db.at(k).reveal();
  }

  void release_rlock(const string& txn_id, const K &k) {
    if (db.find(k) != db.end()) {
      db.at(k).release_rlock(txn_id);
    }
  }

  void put(const string& txn_id, const K &k, const string &v,
           AnnaError &error) {
    bool acquired_lock;
    if (db.find(k) == db.end()) {
      db[k] = LockElement(v);
    }
    if (db.at(k).acquire_wlock(txn_id)) {
      db.at(k).update_temp_value(v);
    } else {
      error = AnnaError::FAILED_OP;
    }
  }

  void commit(const string& txn_id, const K &k, AnnaError &error) {
    // check this key exists
    if (db.find(k) == db.end()) {
      error = AnnaError::FAILED_OP;
      return;
    }

    if (db.at(k).holds_wlock(txn_id)) {
      db.at(k).update_value();
      db.at(k).release_wlock(txn_id);
    } else if (db.at(k).holds_rlock(txn_id)) {
      db.at(k).release_rlock(txn_id);
    } else {
      // TODO(@accheng): should there be an error if nothing to commit?
    }
  }

  void release_wlock(const string& txn_id, const K &k) {
    if (db.find(k) != db.end()) {
      db.at(k).release_wlock(txn_id);
    }
  }

  unsigned size() { return db.size(); }

  void remove(const K &k) { db.erase(k); }
};


#endif // INCLUDE_TXN_BASE_STORE_HPP_