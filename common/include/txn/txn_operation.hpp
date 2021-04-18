#ifndef INCLUDE_TXN_TXN_OPERATION_HPP_
#define INCLUDE_TXN_TXN_OPERATION_HPP_

#include <string>

// template <typename T>
class Operation {
 protected:
  // int type;
  string txn_id;
  Key key;
  string value;

  // T element;
  // virtual void do_merge(const T &e) = 0;

 public:
  Operation() {}
  
  Operation(const string &txn_id, const Key &k,
            const string &v) { assign(txn_id, k, v); }

  Operation(const Operation &other) { 
    assign(other.get_txn_id(), other.get_key(), other.get_value()); 
  }

  virtual ~Operation() = default;
  Operation &operator=(const Operation &rhs) {
    assign(rhs.get_txn_id(), rhs.get_key(), rhs.get_value());
    return *this;
  }

  bool operator==(const Operation &rhs) const {
    return this->get_txn_id() == rhs.get_txn_id() && 
           this->get_key() == rhs.get_key() && 
           this->get_value() == rhs.get_value();
  }

  const string get_txn_id() const {return txn_id; }

  const Key get_key() const { return key; }

  const string get_value() const { return value; }

  void assign(const string t, const Key k,
              const string v) { 
    txn_id = t;
    key = k;
    value = v;
  }

  // void merge(const T &e) { return do_merge(e); }

  // void merge(const Operation<T> &e) { return do_merge(e.reveal()); }

};

#endif  // INCLUDE_TXN_TXN_OPERATION_HPP_
