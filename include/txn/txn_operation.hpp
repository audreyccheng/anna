#ifndef INCLUDE_TXN_TXN_OPERATION_HPP_
#define INCLUDE_TXN_TXN_OPERATION_HPP_

#include <string>

template <typename T>
class Operation {
 protected:
  // int type;
  Key key;
  T value;

  // T element;
  // virtual void do_merge(const T &e) = 0;

 public:
  Operation<T>(const Key &k, const T &v) { assign(k, v); }

  Operation<T>(const Operation<T> &other) { assign(other.get_key(), other.get_value()); }

  virtual ~Operation<T>() = default;
  Operation<T> &operator=(const Operation<T> &rhs) {
    assign(other.get_key(), other.get_value());
    return *this;
  }

  bool operator==(const Operation<T> &rhs) const {
    return this->get_key() == rhs.get_key() && this->get_value() == rhs.get_value();
  }

  const Key get_key() const { return key; }

  const T get_value() const { return value; }

  void assign(const Key k, const T v) { 
    key = k;
    value = v;
  }

  // void merge(const T &e) { return do_merge(e); }

  // void merge(const Operation<T> &e) { return do_merge(e.reveal()); }

};

#endif  // INCLUDE_TXN_TXN_OPERATION_HPP_
