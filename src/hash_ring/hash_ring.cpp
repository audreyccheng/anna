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

#include "hash_ring.hpp"

#include <random>
#include <unistd.h>

#include "requests.hpp"

// get all threads responsible for a key from the "node_type" tier
// metadata flag = 0 means the key is  metadata; otherwise, it is  regular data
ServerThreadList HashRingUtil::get_responsible_threads(
    Address response_address, const Key &key, bool metadata,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<Key, KeyReplication> &key_replication_map, SocketCache &pushers,
    const vector<Tier> &tiers, bool &succeed, unsigned &seed) {
  if (metadata) {
    succeed = true;
    // TODO(@accheng): where should metadata be kept?
    Tier meta_key_tier;
    if (tiers.size() > 0) {
      meta_key_tier = tiers[0];
    } else {
      meta_key_tier = get_random_tier(); // TODO(@accheng): this should never happen?
    }
    return kHashRingUtil->get_responsible_threads_metadata(
        key, global_hash_rings[meta_key_tier], local_hash_rings[meta_key_tier]);
  } else {
    ServerThreadList result;

    if (key_replication_map.find(key) == key_replication_map.end()) {
      // If this a transaction id, only the txnal tier should be responsible for it
      if (tiers.size() > 0 && tiers[0] == Tier::TXN) {
        kHashRingUtil->issue_replication_factor_request(
            response_address, key, tiers[0], global_hash_rings[Tier::TXN],
            local_hash_rings[Tier::TXN], pushers, seed);
      } else { // TODO(@accheng): set which tier to replicate key at
        Tier key_tier = get_random_tier();
        kHashRingUtil->issue_replication_factor_request(
            response_address, key, key_tier, global_hash_rings[key_tier],
            local_hash_rings[key_tier], pushers, seed);
      }
      succeed = false;
    } else {
      for (const Tier &tier : tiers) {
        ServerThreadList threads = responsible_global(
            key, key_replication_map[key].global_replication_[tier],
            global_hash_rings[tier]);

        for (const ServerThread &thread : threads) {
          Address public_ip = thread.public_ip();
          Address private_ip = thread.private_ip();
          set<unsigned> tids = responsible_local(
              key, key_replication_map[key].local_replication_[tier],
              local_hash_rings[tier]);

          for (const unsigned &tid : tids) {
            result.push_back(ServerThread(public_ip, private_ip, tid));
          }
        }
      }

      succeed = true;
    }
    return result;
  }
}

Tier get_random_tier() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(0, 1);
  if (distr(gen) == 0) {
    return Tier::MEMORY;
  }
  return Tier::DISK;
}

// assuming the replication factor will never be greater than the number of
// nodes in a tier return a set of ServerThreads that are responsible for a key
ServerThreadList responsible_global(const Key &key, unsigned global_rep,
                                    GlobalHashRing &global_hash_ring) {
  ServerThreadList threads;
  auto pos = global_hash_ring.find(key);

  if (pos != global_hash_ring.end()) {
    // iterate for every value in the replication factor
    unsigned i = 0;

    while (i < global_rep) {
      if (std::find(threads.begin(), threads.end(), pos->second) ==
          threads.end()) {
        threads.push_back(pos->second);
        i += 1;
      }
      if (++pos == global_hash_ring.end()) {
        pos = global_hash_ring.begin();
      }
    }
  }

  return threads;
}

// assuming the replication factor will never be greater than the number of
// worker threads return a set of tids that are responsible for a key
set<unsigned> responsible_local(const Key &key, unsigned local_rep,
                                LocalHashRing &local_hash_ring) {
  set<unsigned> tids;
  auto pos = local_hash_ring.find(key);

  if (pos != local_hash_ring.end()) {
    // iterate for every value in the replication factor
    unsigned i = 0;

    while (i < local_rep) {
      bool succeed = tids.insert(pos->second.tid()).second;
      if (++pos == local_hash_ring.end()) {
        pos = local_hash_ring.begin();
      }

      if (succeed) {
        i += 1;
      }
    }
  }

  return tids;
}

Address prepare_metadata_request(const Key &key,
                                 GlobalHashRing &global_memory_hash_ring,
                                 LocalHashRing &local_memory_hash_ring,
                                 map<Address, KeyRequest> &addr_request_map,
                                 Address response_address, unsigned &rid,
                                 RequestType type) {
  auto threads = kHashRingUtil->get_responsible_threads_metadata(
      key, global_memory_hash_ring, local_memory_hash_ring);

  if (threads.size() != 0) { // In case no servers have joined yet.
    Address target_address = std::next(begin(threads), rand() % threads.size())
                                 ->key_request_connect_address();
    if (addr_request_map.find(target_address) == addr_request_map.end()) {
      addr_request_map[target_address].set_type(type);
      addr_request_map[target_address].set_response_address(response_address);
      // NB: response_address might not be necessary here
      // (or in other places where req_id is constructed either).
      string req_id = response_address + ":" + std::to_string(rid);
      addr_request_map[target_address].set_request_id(req_id);
      rid += 1;
    }

    return target_address;
  }

  return string();
}

void prepare_metadata_get_request(const Key &key,
                                  GlobalHashRing &global_memory_hash_ring,
                                  LocalHashRing &local_memory_hash_ring,
                                  map<Address, KeyRequest> &addr_request_map,
                                  Address response_address, unsigned &rid) {
  Address target_address = prepare_metadata_request(
      key, global_memory_hash_ring, local_memory_hash_ring, addr_request_map,
      response_address, rid, RequestType::GET);

  if (!target_address.empty()) {
    prepare_get_tuple(addr_request_map[target_address], key, LatticeType::LWW);
  }
}

void prepare_metadata_put_request(const Key &key, const string &value,
                                  GlobalHashRing &global_memory_hash_ring,
                                  LocalHashRing &local_memory_hash_ring,
                                  map<Address, KeyRequest> &addr_request_map,
                                  Address response_address, unsigned &rid) {
  Address target_address = prepare_metadata_request(
      key, global_memory_hash_ring, local_memory_hash_ring, addr_request_map,
      response_address, rid, RequestType::PUT);

  if (!target_address.empty()) {
    auto ts = generate_timestamp(0);
    prepare_put_tuple(addr_request_map[target_address], key, LatticeType::LWW,
                      serialize(ts, value));
  }
}

ServerThreadList HashRingUtilInterface::get_responsible_threads_metadata(
    const Key &key, GlobalHashRing &global_memory_hash_ring,
    LocalHashRing &local_memory_hash_ring) {
  ServerThreadList threads = responsible_global(key, kMetadataReplicationFactor,
                                                global_memory_hash_ring);

  ServerThreadList result;
  for (const ServerThread &thread : threads) {
    Address public_ip = thread.public_ip();
    Address private_ip = thread.private_ip();
    set<unsigned> tids = responsible_local(key, kDefaultLocalReplication,
                                           local_memory_hash_ring);

    for (const unsigned &tid : tids) {
      result.push_back(ServerThread(public_ip, private_ip, tid));
    }
  }

  return result;
}

void HashRingUtilInterface::issue_replication_factor_request(
    const Address &response_address, const Key &key, const Tier &tier,
    GlobalHashRing &global_memory_hash_ring,
    LocalHashRing &local_memory_hash_ring, SocketCache &pushers,
    unsigned &seed) {
  Key replication_key = get_metadata_key(key, MetadataType::replication);
  auto threads = kHashRingUtil->get_responsible_threads_metadata(
      replication_key, global_memory_hash_ring, local_memory_hash_ring);

  Address target_address;
  // TODO(@accheng): change from random thread to primary one
  if (tier == Tier::TXN) {
    target_address = std::next(begin(threads), rand_r(&seed) % threads.size())
      ->txn_request_connect_address();
  } else {
    target_address = std::next(begin(threads), rand_r(&seed) % threads.size()) // --> these threads should only be storage threads, NOT txn threads
      ->storage_request_connect_address();
          // ->key_request_connect_address(); // -> this should be storage_request_connect_address()
  }

  // KeyRequest key_request;
  // key_request.set_type(RequestType::GET);
  // key_request.set_response_address(response_address);

  // prepare_get_tuple(key_request, replication_key, LatticeType::LWW);
  // string serialized;
  // key_request.SerializeToString(&serialized);


  TxnRequest key_request;
  // TODO(@accheng): different init for different tiers?
  if (tier == Tier::TXN) {
    key_request.set_type(RequestType::START_TXN);
  } else {
    key_request.set_type(RequestType::TXN_GET);
  }
  key_request.set_response_address(response_address);

  prepare_txn_get_tuple(key_request, replication_key);
  string serialized;
  key_request.SerializeToString(&serialized);
  kZmqUtil->send_string(serialized, &pushers[target_address]);
}

