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

#include "route/routing_handlers.hpp"

void replication_response_handler(
    logger log, string &serialized, SocketCache &pushers, RoutingThread &rt,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<Key, KeyReplication> &key_replication_map,
    map<Key, vector<pair<Address, string>>> &pending_requests, unsigned &seed) {
  TxnResponse response;
  response.ParseFromString(serialized);
  // we assume tuple 0 because there should only be one tuple responding to a
  // replication factor request
  TxnKeyTuple tuple = response.tuples(0);

  Key key = get_key_from_metadata(tuple.key()); // TODO(@accheng): update?

  AnnaError error = tuple.error();

  bool txn_tier = false;
  if (response.type() == Tier::TXN) {
    txn_tier = true;
  }

  if (error == AnnaError::NO_ERROR) {
    LWWValue lww_value;
    lww_value.ParseFromString(tuple.payload());
    ReplicationFactor rep_data;
    rep_data.ParseFromString(lww_value.value());

    for (const auto &global : rep_data.global()) {
      key_replication_map[key].global_replication_[global.tier()] =
          global.value();
    }

    for (const auto &local : rep_data.local()) {
      key_replication_map[key].local_replication_[local.tier()] = local.value();
    }
  } else if (error == AnnaError::KEY_DNE) {
    // this means that the receiving thread was responsible for the metadata
    // but didn't have any values stored -- we use the default rep factor
    init_replication(key_replication_map, key, txn_tier);
  } else if (error == AnnaError::WRONG_THREAD) {
    // this means that the node that received the rep factor request was not
    // responsible for that metadata
    auto respond_address = rt.replication_response_connect_address();
    if (txn_tier) {
      kHashRingUtil->issue_replication_factor_request(
        respond_address, key, global_hash_rings[Tier::TXN],
        local_hash_rings[Tier::TXN], pushers, seed);
    } else {
      kHashRingUtil->issue_replication_factor_request(
        respond_address, key, global_hash_rings[Tier::MEMORY],
        local_hash_rings[Tier::MEMORY], pushers, seed);
    }
    return;
  } else {
    log->error("Unexpected error type {} in replication factor response.",
               error);
    return;
  }

  // process pending key address requests
  if (pending_requests.find(key) != pending_requests.end()) {
    bool succeed;
    ServerThreadList threads = {};

    for (const Tier &tier : kAllTiers) {
      // Only txn tier should serve txn requests and vice versa for storage tiers
      if (txn_tier && tier != Tier::TXN || 
         !txn_tier && tier == Tier::TXN) {
        continue;
      }

      threads = kHashRingUtil->get_responsible_threads(
        rt.replication_response_connect_address(), key, false, txn_tier,
        global_hash_rings, local_hash_rings, key_replication_map, pushers,
        {tier}, succeed, seed);

      if (threads.size() > 0) {
        break;
      }

      if (!succeed) {
        log->error("Missing replication factor for key {}.", key);
        return;
      }
    }

    for (const auto &pending_key_req : pending_requests[key]) {
      KeyAddressResponse key_res;
      key_res.set_response_id(pending_key_req.second);
      auto *tp = key_res.add_addresses();
      tp->set_key(key);

      for (const ServerThread &thread : threads) {
        tp->add_ips(thread.key_request_connect_address());
      }

      string serialized;
      key_res.SerializeToString(&serialized);
      kZmqUtil->send_string(serialized, &pushers[pending_key_req.first]);
    }

    pending_requests.erase(key);
  }
}
