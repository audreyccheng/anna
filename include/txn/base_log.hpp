#ifndef INCLUDE_TXN_BASE_LOG_HPP_
#define INCLUDE_TXN_BASE_LOG_HPP_

#include "anna.pb.h"
#include "types.hpp"

// Basic class for storage node using map<key, value>
template <typename T> class BaseLog {
protected:
  vector<T> log;
  // virtual void get(const K &k, AnnaError &error) = 0;
  // virtual void put(const K &k, const V &v) = 0;

public:
  BaseLog<T>() {}

  BaseLog<T>(vector<T> &other) { log = other; }

  // append an entry and return the log position it occupies
  unsigned append(const V &v) { 
    log.push_back(v);
    return log.size() - 1;
  }

  unsigned size() { return log.size(); }

  // delete records up to log position l
  void trim(const unsigned &l) { log.erase(0, l); }

  V read(const unsigned &l, AnnaError &error) {
    if (l >= log.size()) {
      error = AnnaError::LOG_DNE;
      return NULL;
    }

    return log.at(l);
  }

  // return all records at and after log position l
  vector<V> subscribe(const unsigned &l, AnnaError &error) {
    vector<V> vec;
    if (l >= log.size()) {
      error = AnnaError::LOG_DNE;
      return vec;
    }

    auto index = l;
    while (index < log.size()) {
      vec.push_back(l.at(index));
      index++;
    }
    return vec;
  }
};


#endif // INCLUDE_TXN_BASE_LOG_HPP_