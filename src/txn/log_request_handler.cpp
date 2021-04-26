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

#include "txn/txn_handlers.hpp"

void log_request_handler(
    unsigned &access_count, unsigned &seed, string &serialized, logger log,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<Key, vector<PendingTxnRequest>> &pending_requests, // <key, <request_id, PendingTxnRequest>> 
    map<Key, std::multiset<TimePoint>> &key_access_tracker,
    map<Key, TxnKeyProperty> &stored_key_map, // <key, TxnKeyProperty>
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, LogSerializer *serializer,
    SocketCache &pushers) {
  TxnRequest request;
  request.ParseFromString(serialized);

  TxnResponse response;
  string response_id = request.request_id();
  response.set_response_id(request.request_id());
  response.set_txn_id(request.txn_id());

  response.set_type(request.type());
  response.set_tier(get_anna_tier_from_tier(kSelfTier));

  bool succeed;
  RequestType request_type = request.type();
  string response_address = request.response_address();
  string txn_id = request.txn_id();

  for (const auto &tuple : request.tuples()) {
    // first check if the thread is responsible for the key
    Key key = tuple.key();
    string payload = tuple.payload();

    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
        wt.replication_response_connect_address(), key, is_metadata(key),
        global_hash_rings, local_hash_rings, key_replication_map, pushers,
        kSelfTierIdVector, succeed, seed);

    if (succeed) {
      if (std::find(threads.begin(), threads.end(), wt) == threads.end()) {
        if (is_metadata(key)) {
          // this means that this node is not responsible for this metadata key
          TxnKeyTuple *tp = response.add_tuples();

          tp->set_key(key);
          tp->set_error(AnnaError::WRONG_THREAD);
        } else {
          // if we don't know what threads are responsible, we issue a rep
          // factor request and make the request pending
          kHashRingUtil->issue_replication_factor_request(
              wt.replication_response_connect_address(), key, kSelfTier,
              global_hash_rings[kSelfTier], local_hash_rings[kSelfTier],
              pushers, seed);

          pending_requests[key].push_back( 
              PendingTxnRequest(request_type, txn_id, key, payload,
                                response_address, response_id));
        }
      } else { // if we know the responsible threads, we process the request
        if (request_type == RequestType::PREPARE_TXN || 
            request_type == RequestType::COMMIT_TXN) {
          TxnKeyTuple *tp = response.add_tuples();
          tp->set_key(key);

          AnnaError error = AnnaError::NO_ERROR;
          process_log(txn_id, key, payload, error, serializer); // TODO(@accheng): update
          tp->set_error(error);

          if (tuple.address_cache_size() > 0 &&
              tuple.address_cache_size() != threads.size()) {
            tp->set_invalidate(true);
          }
        } else {
          log->error("Unknown request type {} in user request handler.",
                     request_type);
        }

        key_access_tracker[key].insert(std::chrono::system_clock::now());
        access_count += 1;
      }
    } else {
      pending_requests[key].push_back(
          PendingTxnRequest(request_type, txn_id, key, payload,
                            response_address, response_id));
    }
  }

  if (response.tuples_size() > 0 && request.response_address() != "") {
    string serialized_response;
    response.SerializeToString(&serialized_response);
    kZmqUtil->send_string(serialized_response,
                          &pushers[request.response_address()]);
  }
}
