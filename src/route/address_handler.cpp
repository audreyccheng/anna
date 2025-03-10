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

void address_handler(logger log, string &serialized, SocketCache &pushers,
                     RoutingThread &rt, GlobalRingMap &global_hash_rings,
                     LocalRingMap &local_hash_rings,
                     map<Key, KeyReplication> &key_replication_map,
                     map<Key, vector<pair<Address, string>>> &pending_requests,
                     unsigned &seed) {
  KeyAddressRequest addr_request;
  addr_request.ParseFromString(serialized);
  Tier addr_tier = get_tier_from_anna_tier(addr_request.tier());

  KeyAddressResponse addr_response;
  addr_response.set_response_id(addr_request.request_id());
  addr_response.set_tier(addr_request.tier());
  bool succeed;

  int num_servers = 0;
  for (const auto &pair : global_hash_rings) {
    num_servers += pair.second.size();
  }

  bool respond = false;
  if (num_servers == 0) {
    addr_response.set_error(AnnaError::NO_SERVERS);

    for (const Key &key : addr_request.keys()) {
      KeyAddressResponse_KeyAddress *tp = addr_response.add_addresses();
      tp->set_key(key);
    }

    respond = true;
  } else { // if there are servers, attempt to return the correct threads
    for (const Key &key : addr_request.keys()) {
      ServerThreadList threads = {};

      log->info("Routing address handler received for key {}", key);

      if (key.length() > 0) { // Only run this code is the key is a valid string.
        // Otherwise, an empty response will be sent.
        for (const Tier &tier : kAllTiers) {
          // Only txn tier should serve txn requests and vice versa for storage tiers
          if (addr_tier == Tier::TXN && tier != Tier::TXN || 
              addr_tier != Tier::TXN && tier == Tier::TXN) {
            continue;
          }
          RequestType request_type = RequestType::START_TXN;
          if (tier != Tier::TXN) {
            request_type = RequestType::TXN_GET;
          }
          threads = kHashRingUtil->get_responsible_threads(
              rt.replication_response_connect_address(), request_type, // TODO(@accheng): what type should this be?
              "" /* txn_id */, key, is_metadata(key), 
              global_hash_rings, local_hash_rings, key_replication_map,
              pushers, {tier}, succeed, seed, log);

          if (threads.size() > 0) {
            break;
          }

          if (!succeed) { // this means we don't have the replication factor for
                          // the key
            pending_requests[key].push_back(std::pair<Address, string>(
                addr_request.response_address(), addr_request.request_id()));
            return;
          }
        }
      }

      KeyAddressResponse_KeyAddress *tp = addr_response.add_addresses();
      tp->set_key(key);
      respond = true;

      // TODO(@accheng): should just be 1 thread? separate method for primary thread?
      for (const ServerThread &thread : threads) {
        // send transaction or storage request handler addresses
        if (addr_tier == Tier::TXN) {
          tp->add_ips(thread.txn_request_connect_address());
        } else {
          tp->add_ips(thread.storage_request_connect_address());
        }
        // TODO(@accheng): would we ever want the logging handler?
      }
    }
  }

  if (respond) {
    log->info("Routing address handler sending response");
    string serialized;
    addr_response.SerializeToString(&serialized);

    kZmqUtil->send_string(serialized,
                          &pushers[addr_request.response_address()]);
  }
}
