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

  const bool is_used() const {
    return rlocks.size() > 0 || wlock.length() > 0;
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

  void abort(const string& txn_id, const K &k, AnnaError &error) {
    std::cout << txn_id << " serializer abort" << std::endl;
    // check this key exists
    if (db.find(k) == db.end()) {
      error = AnnaError::FAILED_OP;
      return;
    }

    if (db.at(k).holds_wlock(txn_id)) {
      std::cout << txn_id << " release wlock on " << k << std::endl;
      db.at(k).release_wlock(txn_id);
    } else if (db.at(k).holds_rlock(txn_id)) {
      std::cout << txn_id << " release wlock on " << k << std::endl;
      db.at(k).release_rlock(txn_id);
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

template <typename K> class DiskLockNode {
protected:
  map<K, LockElement> db;
  unsigned thread_id;
  string ebs_root;

public:
  DiskLockNode<K>(unsigned &thread_id, const string &ebs_root) {
    this->thread_id = thread_id;
    this->ebs_root = ebs_root;
  }

  /**
   * Reads the value of key k from disk into in-memory map. 
   * Returns true if success, false if key doesn't exist on disk.
   */
  bool read_disk(const K &k) {
    // Key not being used right now, check if stored in disk
    string fname = ebs_root + std::to_string(thread_id) + "/" + k;
    std::fstream f(fname, std::ios::in | std::ios::binary);
    if (f) {
      // Key exists on disk, read it into the map
      // Stored as <is_primary>\n<value>
      string is_primary;
      string element;
      getline(f, is_primary);
      getline(f, element);

      db[k] = LockElement(element, is_primary.at(0) == '1');
      db.at(k).assign_primary(is_primary.at(0) == '1');
      db.at(k).update_temp_value(element);
      db.at(k).update_value();
      f.close();
      return true;
    }
    f.close();

    return false;
  }

  /**
   * Removes key k from in-memory map iff it is no longer used. Writes value to disk.
   */
  void purge_key(const K &k) {
    std::cout << "purging key " << k << std::endl;
    if (db.find(k) != db.end() && !db.at(k).is_used()) {
      if (db.at(k).reveal().length() == 0) {
        // Empty value, remove from disk
        remove(k);
        return;
      }

      std::cout << "not empty, writing back to disk " << std::endl;
      // Write it back to disk
      string fname = ebs_root + std::to_string(thread_id) + "/" + k;
      std::fstream f(fname, std::ios::out | std::ios::binary);
      f << db.at(k).get_is_primary() << "\n" << db.at(k).reveal() << "\n";
      f.close();
      db.erase(k);
    }
  }

  string get(const string& txn_id, const K &k, AnnaError &error) {
    if (db.find(k) == db.end()) {
      // Key not being used right now
      if (!read_disk(k)) {
        // Key not on disk either, doesn't exist
        error = AnnaError::KEY_DNE;
        return "";
      }
    }

    // TODO(@accheng): return some value even if no rlock?
    if (!db.at(k).acquire_rlock(txn_id)) {
      error = AnnaError::FAILED_OP;
    }
    return db.at(k).reveal();
  }

  string reveal_element(const K &k, AnnaError &error) {
    if (db.find(k) == db.end()) {
      // Key not being used right now
      if (!read_disk(k)) {
        // Key not on disk either, doesn't exist
        error = AnnaError::KEY_DNE;
        return "";
      }
    }

    string s = db.at(k).reveal();
    purge_key(k);
    return s;
  }

  string reveal_temp_element(const K &k, AnnaError &error) {
    if (db.find(k) == db.end()) {
      // Key not being used right now
      if (!read_disk(k)) {
        // Key not on disk either, doesn't exist
        error = AnnaError::KEY_DNE;
        return "";
      }
    }

    string s = db.at(k).temp_reveal();
    purge_key(k);
    return s;
  }

  void release_rlock(const string& txn_id, const K &k) {
    if (db.find(k) != db.end()) {
      db.at(k).release_rlock(txn_id);
      purge_key(k);
    }
  }

  bool get_is_primary(const K &k, AnnaError &error) {
    if (db.find(k) == db.end()) {
      // Key not being used right now
      if (!read_disk(k)) {
        // Key not on disk either, doesn't exist
        error = AnnaError::FAILED_OP;
        return false;
      }
    }

    bool is_primary = db.at(k).get_is_primary();
    purge_key(k);
    return is_primary;
  }

  void put(const string& txn_id, const K &k, const string &v,
           AnnaError &error, const bool &is_primary) {
    if (db.find(k) == db.end()) {
      db[k] = LockElement("", is_primary);
      db.at(k).assign_primary(is_primary);
    }

    if (db.at(k).acquire_wlock(txn_id)) {
      db.at(k).update_temp_value(v);
    } else {
      error = AnnaError::FAILED_OP;
    }
  }

  void commit(const string& txn_id, const K &k, AnnaError &error) {
    // Check this key is being used
    if (db.find(k) == db.end()) {
      error = AnnaError::FAILED_OP;
      return;
    }

    std::cout << "Committing " << k << std::endl;
    if (db.at(k).holds_wlock(txn_id)) {
      std::cout << "had write lock" << std::endl;
      db.at(k).update_value();
      release_wlock(txn_id, k);
    } else if (db.at(k).holds_rlock(txn_id)) {
      std::cout << "had read lock" << std::endl;
      release_rlock(txn_id, k);
    }
  }

  void abort(const string& txn_id, const K &k, AnnaError &error) {
    std::cout << txn_id << " serializer abort" << std::endl;
    // check this key exists
    if (db.find(k) == db.end()) {
      error = AnnaError::FAILED_OP;
      return;
    }

    if (db.at(k).holds_wlock(txn_id)) {
      release_wlock(txn_id, k);
    } else if (db.at(k).holds_rlock(txn_id)) {
      release_rlock(txn_id, k);
    }
  }

  void release_wlock(const string& txn_id, const K &k) {
    if (db.find(k) != db.end()) {
      std::cout << txn_id << " release wlock on " << k << std::endl;
      db.at(k).release_wlock(txn_id);
      purge_key(k);
    }
  }

  unsigned size() { return db.size(); }

  void remove(const K &k) {
    db.erase(k);
    std::remove((ebs_root + "/" + std::to_string(thread_id) + "/" + k).c_str());
  }
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
    wts = tts; // Equals tts oof txn that wrote this, or 0 for initial version
    rts = tts;
    visibility = tts; // Equals tts of txn that can see this, or -1 for globally visible
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

  const bool is_initial_version() {
    return wts == 0;
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
  string::size_type n_rand;

  n_rand = txn_id.find(":"); // TODO(@accheng): update to constant
  string tts_string = txn_id.substr(n_rand + 1);
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
    // Get should always get most recent, visible version, even if this txn wrote that version.
    MVCCVersion *snapshot = get_snapshot(tts, k, false);

    if (snapshot->is_initial_version()) {
      // Key doesn't yet exist, we're reading the initial "empty" version
      error = AnnaError::KEY_DNE;
    }

    snapshot->update_rts(tts);
    return snapshot->read();
  }

  bool get_is_primary(const K &k, AnnaError &error) {
    // Uses the latest, globally visible version
    MVCCVersion *snapshot = get_snapshot(LONG_MAX, k, true);

    if (snapshot->is_initial_version()) {
      // Key doesn't yet exist, we're reading the initial "empty" version
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
      // Key doesn't yet exist; create initial, globally visible, empty version
      return initialize_key(k);
    }
    
    for (MVCCVersion &version : db.at(k)) {
      if (version.visible_by(tts) && (!original || version.globally_visible())) {
        return &version;
      }
    }

    // Should never reach here
    return NULL;
  }

  /**
   * Creates an "empty", globally visible version to represent "key doesn't exist". Needed to maintain consistency via MVCC rules.
   */
  MVCCVersion* initialize_key(const K &k) {
    db[k] = { MVCCVersion(0, "", true) }; // TODO: for these initial versions, what should is_primary be?
    MVCCVersion* initial = &db.at(k)[0];
    initial->make_globally_visible();
    return initial;
  }

  void put(const string& txn_id, const K &k, const string &v,
           AnnaError &error, const bool &is_primary) {
    long tts = get_tts(txn_id);
    if (db.find(k) == db.end()) {
      // Key doesn't yet exist; create initial, globally visible, empty version
      initialize_key(k);
    }
    
    // Check if we are allowed to write to this key
    MVCCVersion *original_snapshot = get_snapshot(tts, k, true);
    if (!original_snapshot->write_allowed(tts)) {
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

  void commit(const string& txn_id, const K &k, AnnaError &error) {
    long tts = get_tts(txn_id);
    MVCCVersion *snapshot = get_snapshot(tts, k, false);

    // check this key exists
    if (snapshot->is_initial_version()) {
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