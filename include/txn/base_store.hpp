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
  bool is_primary;

 public:
  LockElement() {
    element = "";
    temp_element = "";
    wlock = "";
    is_primary = false;
  }

  LockElement(const string &e, const bool &primary) { 
    assign(e); 
    assign_primary(primary);
  }

  LockElement(const LockElement &other) { 
    assign(other.reveal()); 
    assign_primary(other.get_is_primary());
  }

  // virtual ~LockElement() = default;
  LockElement &operator=(const LockElement &rhs) {
    assign(rhs.reveal());
    return *this;
  }

  bool operator==(const LockElement &rhs) const {
    return this->reveal() == rhs.reveal();
  }

  const string &reveal() const { return element; }

  const string &temp_reveal() const { return temp_element; }

  void assign(const string e) { temp_element = e; }

  void assign_primary(const bool &primary) {
    is_primary = primary;
  }

  void assign(const LockElement &e) { 
    temp_element = e.reveal();
    is_primary = e.get_is_primary();
  }

  bool acquire_rlock(const string& txn_id) {
    if (wlock == "") {
      rlocks.insert(txn_id);
      return true;
    }
    return false;
  }

  const bool &get_is_primary() const { return is_primary; }

  bool acquire_wlock(const string& txn_id) {
    if (rlocks.empty() && wlock == "" && is_primary) {
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

  string reveal_element(const K &k, AnnaError &error) {
    if (db.find(k) == db.end()) {
      error = AnnaError::KEY_DNE;
      return ""; 
    }
    return db.at(k).reveal();
  }

  string reveal_temp_element(const K &k, AnnaError &error) {
    if (db.find(k) == db.end()) {
      error = AnnaError::KEY_DNE;
      return ""; 
    }
    return db.at(k).temp_reveal();
  }

  void release_rlock(const string& txn_id, const K &k) {
    if (db.find(k) != db.end()) {
      db.at(k).release_rlock(txn_id);
    }
  }

  bool get_is_primary(const K &k, AnnaError &error) {
    if (db.find(k) != db.end()) {
      return db.at(k).get_is_primary();
    } else {
      error = AnnaError::FAILED_OP;
      return false;
    }
  }

  void put(const string& txn_id, const K &k, const string &v,
           AnnaError &error, const bool &is_primary) {
    if (db.find(k) == db.end()) {
      db[k] = LockElement(v, is_primary);
      db.at(k).assign_primary(is_primary);
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

// template <typename T>
class MVCCVersion {
 protected:
  string value;
  long wts;
  long rts;
  bool is_primary;
  long visibility;

 public:
  MVCCVersion(const long &tts, const string &e, const bool &primary) { 
    wts = tts;
    rts = tts;
    visibility = tts;
    value = e;
    is_primary = primary;
  }

  const string &read() const { 
    return value;
  }

  const bool &get_is_primary() const { return is_primary; }


  const bool write_allowed(const long &tts) {
    return rts <= tts;
  }

  const bool visible_by(const long &tts) {
    return (visibility == tts || visibility == -1) && wts <= tts;
  }

  const bool globally_visible() {
    return visibility == -1;
  }

  void update_rts(const long &tts) { 
    rts = std::max(tts, rts);
  }

  void assign(const string &v) {
    value = v;
  }

  void assign_primary(const bool &primary) {
    is_primary = primary;
  }

  void make_globally_visible() {
    visibility = -1;
  }
};

inline long get_tts(const string& txn_id) {
  string::size_type n_id;
  string::size_type n_time;

  n_id = txn_id.find(":"); // TODO(@accheng): update to constant
  string tts_string = txn_id.substr(n_id + 1);
  return stol(tts_string);
}

template <typename K> class MVCCNode {
protected:
  // Versions stored in reverse chronological order
  map<K, vector<MVCCVersion>> db;

public:
  MVCCNode<K>() {}

  MVCCNode<K>(map<K, vector<MVCCVersion>> &other) { db = other; }

  string get(const string& txn_id, const K &k, AnnaError &error) {
    long tts = get_tts(txn_id);
    /**
     * Get should always get most recent, visible version, even if this txn was the one
     * who wrote that version.
     */
    MVCCVersion *snapshot = get_snapshot(tts, k, false);

    if (snapshot == NULL) {
      // Key never written to before this transaction
      error = AnnaError::KEY_DNE;
      return "";
    }
    
    return snapshot->read();
  }

  bool get_is_primary(const K &k, AnnaError &error) {
    if (db.find(k) == db.end()) {
      std::cout << "Can't find key k" << std::endl;
      error = AnnaError::FAILED_OP;
      return false;
    }

    MVCCVersion *snapshot = get_snapshot(LONG_MAX, k, true);
    if (snapshot == NULL) {
      std::cout << "No snapshot available" << std::endl;
      error = AnnaError::FAILED_OP;
      return false;
    }

    return snapshot->get_is_primary();
  }

  /**
   * Gets a relevant snapshot for key k for this transaction.
   * If original, gets the version that this transaction originally read from (i.e. latest, globally-visible version with wts <= tts).
   * If !original, gets the latest version visible to this txn (may be a new version in-progress, written by this txn).
   */
  MVCCVersion* get_snapshot(const long &tts, const K &k, const bool &original) {
    if (db.find(k) == db.end()) {
      return NULL; 
    }

    
    for (MVCCVersion &version : db.at(k)) {
      std::cout << version.read() << std::endl;
      std::cout << version.get_is_primary() << std::endl;
      if (version.visible_by(tts) && (!original || version.globally_visible())) {
        version.update_rts(tts);
        return &version;
      }
    }

    return NULL;
  }

  void put(const string& txn_id, const K &k, const string &v,
           AnnaError &error, const bool &is_primary) {
    long tts = get_tts(txn_id);
    if (db.find(k) == db.end()) {
      // Key doesn't exist in db yet
      db[k] = { MVCCVersion(tts, v, is_primary) };
    } else {
      // Key exists in db

      // Check if we are allowed to write to this key
      MVCCVersion *original_snapshot = get_snapshot(tts, k, true);
      if (original_snapshot != NULL && !original_snapshot->write_allowed(tts)) {
        error = AnnaError::FAILED_OP;
        return;
      }


      MVCCVersion *latest_visible_snapshot = get_snapshot(tts, k, false);
      if (!latest_visible_snapshot->globally_visible()) {
        // If this txn already created a new version, just modify that version directly (it's visible only to us anyways)
        latest_visible_snapshot->assign(v);
        latest_visible_snapshot->assign_primary(is_primary);
      } else {
        // ... otherwise, create a new version
        db.at(k).insert(db.at(k).begin(), MVCCVersion(tts, v, is_primary));
      }
    }
  }

  void commit(const string& txn_id, const K &k, AnnaError &error) {
    long tts = get_tts(txn_id);
    MVCCVersion *snapshot = get_snapshot(tts, k, false);

    // check this key exists
    if (snapshot == NULL) {
      error = AnnaError::FAILED_OP;
      return;
    }

    // if currently only visible to us, make visible to everyone
    if (!(snapshot->globally_visible())) {
      snapshot->make_globally_visible();
    } else {
      // TODO: should there be an error if nothing to commit?
    }
  }

  unsigned size() { return db.size(); }

  void remove(const K &k) { db.erase(k); }
};


#endif // INCLUDE_TXN_BASE_STORE_HPP_