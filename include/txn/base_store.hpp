#ifndef INCLUDE_TXN_BASE_STORE_HPP_
#define INCLUDE_TXN_BASE_STORE_HPP_

#include "anna.pb.h"
#include "types.hpp"

// Basic class for storage node using map<key, value>
template <typename K, typename V> class BaseStore {
protected:
  Map<K, V> db;
  // virtual void get(const K &k, AnnaError &error) = 0;
  // virtual void put(const K &k, const V &v) = 0;

public:
  BaseStore<K, V>() {}

  BaseStore<K, V>(Map<K, V> &other) { db = other; }

  V get(const K &k, AnnaError &error) {
    if (!db.contains(k).reveal()) {
      error = AnnaError::KEY_DNE;
    }

    return db.at(k);
  }

  void put(const K &k, const V &v) { return db[k] = v; }

  // unsigned size(const K &k) { return db.at(k).size().reveal(); }

  void remove(const K &k) { db.remove(k); }
};

template <typename T>
class LockElement {
 protected:
  T element;
  // TODO(@accheng): should this be a vector?
  set<string> rlocks;
  string wlock;

 public:
  // LockElement<T>() {}

  LockElement<T>(const T &e) { assign(e); }

  LockElement<T>(const LockElement<T> &other) { assign(other.reveal()); }

  virtual ~LockElement<T>() = default;
  LockElement<T> &operator=(const LockElement<T> &rhs) {
    assign(rhs.reveal());
    return *this;
  }

  bool operator==(const LockElement<T> &rhs) const {
    return this->reveal() == rhs.reveal();
  }

  const T &reveal() const { return element; }

  void assign(const T e) { element = e; }

  void assign(const LockElement<T> &e) { element = e.reveal(); }

  bool rlock(const string& txn_id) {
    if (wlock == "") {
      rlock.insert(txn_id);
      return true;
    }
    return false;
  }

  bool wlock(const string& txn_id) {
    if (rlocks.empty() && wlock == "") {
      wlock = txn_id;
      return true;
    }
    return false;
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

};

template <typename K, LockElement<V>> class LockStore {
protected:
  Map<K, LockElement<V>> db;

public:
  LockStore<K, LockElement<V>>() {}

  LockStore<K, LockElement<V>>(Map<K, LockElement<V>> &other) { db = other; }

  V get(const string& txn_id, const K &k, AnnaError &error) {
    if (!db.contains(k)) {
      error = AnnaError::KEY_DNE;
    } else {
      // TODO(@accheng): return some value even if no rlock?
      if (!db.at(k).rlock(txn_id)) {
        error = AnnaError::FAILED_OP;
      }
    }

    return db[k].reveal();
  }

  void release_rlock(const string& txn_id, const K &k) {
    if (db.contains(k)) {
      db.at(k).release_rlock(txn_id);
    }
  }

  void put(const string& txn_id, const K &k, const V &v, AnnaError &error) {
    bool acquired_lock;
    if (!db.contains(k)) {
      db[k] = LockElement<V>(v);
    }
    if (!db.at(k).wlock(txn_id)) {
      error = AnnaError::FAILED_OP;
    }
  }

  void release_wlock(const string& txn_id, const K &k) {
    if (db.contains(k)) {
      db.at(k).release_wlock(txn_id);
    }
  }

  // unsigned size(const K &k) { return db.at(k).size().reveal(); }

  void remove(const K &k) { db.remove(k); }
};


#endif // INCLUDE_TXN_BASE_STORE_HPP_