#ifndef INCLUDE_TXN_BASE_LOG_HPP_
#define INCLUDE_TXN_BASE_LOG_HPP_

#include "anna.pb.h"
#include "common.hpp"
#include "types.hpp"

// Basic class for storage node using map<key, value>
// template <typename T> 
class BaseLog {
protected:
  vector<Operation> log;
  // virtual void get(const K &k, AnnaError &error) = 0;
  // virtual void put(const K &k, const V &v) = 0;

public:
  BaseLog() {}

  BaseLog(vector<Operation> &other) { log = other; }

  // append an entry and return the log position it occupies
  unsigned append(const Operation &v) { 
    log.push_back(v);
    return log.size() - 1;
  }

  unsigned size() { return log.size(); }

  // delete records up to log position l
  void trim(const unsigned &l) { log.erase(log.begin(), log.begin() + l); }

  Operation read(const unsigned &l, AnnaError &error) {
    if (l >= log.size()) {
      error = AnnaError::LOG_DNE;
      return Operation();
    }

    return log.at(l);
  }

  // return all records at and after log position l
  vector<Operation> subscribe(const unsigned &l, AnnaError &error) {
    vector<Operation> vec;
    if (l >= log.size()) {
      error = AnnaError::LOG_DNE;
      return vec;
    }

    auto index = l;
    while (index < log.size()) {
      vec.push_back(log.at(index));
      index++;
    }
    return vec;
  }
};


#endif // INCLUDE_TXN_BASE_LOG_HPP_