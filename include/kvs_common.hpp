//  Copyright 2019 U.C. Berkeley RISE Lab
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#ifndef KVS_INCLUDE_KVS_COMMON_HPP_
#define KVS_INCLUDE_KVS_COMMON_HPP_

#include "kvs_types.hpp"
#include "anna.pb.h"
#include "metadata.pb.h"

#include <random>

const unsigned kMetadataReplicationFactor = 1;
const unsigned kMetadataLocalReplicationFactor = 1;

const unsigned kVirtualThreadNum = 3000;

const vector<Tier> kAllTiers = {
    Tier::MEMORY,
    Tier::DISK,
    Tier::TXN,
    Tier::LOG}; // TODO(vikram): Is there a better way to make this vector?

const vector<Tier> kStorageTiers = {
    Tier::MEMORY,
    Tier::DISK
};

const unsigned kSloWorst = 3000;

// run-time constants
extern Tier kSelfTier;
extern vector<Tier> kSelfTierIdVector;

extern unsigned kTxnNodeCapacity;
extern unsigned kMemoryNodeCapacity;
extern unsigned kEbsNodeCapacity;
extern unsigned kLogNodeCapacity;

// the number of threads running in this executable
extern unsigned kThreadNum;
extern unsigned kTxnThreadCount;
extern unsigned kMemoryThreadCount;
extern unsigned kEbsThreadCount;
extern unsigned kRoutingThreadCount;
extern unsigned kLogThreadCount;

extern unsigned kDefaultGlobalTxnReplication;
extern unsigned kDefaultGlobalMemoryReplication;
extern unsigned kDefaultGlobalEbsReplication;
extern unsigned kDefaultLocalReplication;
extern unsigned kDefaultGlobalLogReplication;
extern unsigned kMinimumReplicaNumber;

inline void prepare_get_tuple(KeyRequest &req, Key key,
                              LatticeType lattice_type) {
  KeyTuple *tp = req.add_tuples();
  tp->set_key(std::move(key));
  tp->set_lattice_type(std::move(lattice_type));
}

inline void prepare_put_tuple(KeyRequest &req, Key key,
                              LatticeType lattice_type, string payload) {
  KeyTuple *tp = req.add_tuples();
  tp->set_key(std::move(key));
  tp->set_lattice_type(std::move(lattice_type));
  tp->set_payload(std::move(payload));
}

inline void prepare_txn_tuple(TxnRequest &req, Key key,
                              string payload) {
  TxnKeyTuple *tp = req.add_tuples();
  tp->set_key(std::move(key));
  if (payload != "") {
    tp->set_payload(std::move(payload));
  }
}

inline void prepare_txn_put_tuple(TxnRequest &req, Key key,
                                  string payload) {
  TxnKeyTuple *tp = req.add_tuples();
  tp->set_key(std::move(key));
  tp->set_payload(std::move(payload));
}

inline Tier get_random_tier() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(0, 1);
  if (distr(gen) == 0) {
    return Tier::MEMORY;
  }
  return Tier::DISK;
}

inline Tier get_tier_from_anna_tier(AnnaTier anna_tier) {
  if (anna_tier == AnnaTier::AMEMORY) {
    return Tier::MEMORY;
  } else if (anna_tier == AnnaTier::ADISK) {
    return Tier::DISK;
  } else if (anna_tier == AnnaTier::AROUTING) {
    return Tier::ROUTING;
  } else if (anna_tier == AnnaTier::ATXN) {
    return Tier::TXN;
  } else if (anna_tier == AnnaTier::ALOG) {
    return Tier::LOG;
  }
  return Tier::TIER_UNSPECIFIED;
}

inline AnnaTier get_anna_tier_from_tier(Tier tier) {
  if (tier == Tier::MEMORY) {
    return AnnaTier::AMEMORY;
  } else if (tier == Tier::DISK) {
    return AnnaTier::ADISK;
  } else if (tier == Tier::ROUTING) {
    return AnnaTier::AROUTING;
  } else if (tier == Tier::TXN) {
    return AnnaTier::ATXN;
  } else if (tier == Tier::LOG) {
    return AnnaTier::ALOG;
  }
  return AnnaTier::ATIER_UNSPECIFIED;
}

#endif // KVS_INCLUDE_KVS_COMMON_HPP_
