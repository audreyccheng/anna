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

void storage_request_handler(
    unsigned &access_count, unsigned &seed, string &serialized, logger log,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<Key, vector<PendingTxnRequest>> &pending_requests, // <key, <request_id, PendingTxnRequest>> 
    map<Key, std::multiset<TimePoint>> &key_access_tracker,
    map<Key, TxnKeyProperty> &stored_key_map, // <key, TxnKeyProperty>
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, BaseSerializer *serializer,
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

    log->info("Received storage_request type {} for txn id {} key {}", request_type, txn_id, key);

    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
        wt.replication_response_connect_address(), request_type, txn_id, 
        key, is_metadata(key),
        global_hash_rings, local_hash_rings, key_replication_map, pushers,
        kSelfTierIdVector, succeed, seed, log);

    string suc = "false";
    if (succeed) {
      suc = "true";
    }
    log->info("Storage request getting threads success: {}, num threads: {}", suc, threads.size());

    bool is_primary = true; // TODO(@accheng): update

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
              wt.replication_response_connect_address(), request_type, txn_id, 
              key, kSelfTier,
              global_hash_rings[kSelfTier], local_hash_rings[kSelfTier],
              pushers, seed, log);

          pending_requests[key].push_back( 
              PendingTxnRequest(request_type, txn_id, key, payload,
                                response_address, response_id));
        }
      } else { // if we know the responsible threads, we process the request
        TxnKeyTuple *tp = response.add_tuples();
        tp->set_key(key);

        // check if this is an replication factor request
        if (is_metadata(key) && stored_key_map.find(key) == stored_key_map.end()) {
          log->info("storage request metadata req txn id {} key {}", txn_id, key);
          tp->set_error(AnnaError::KEY_DNE);
        } else if (request_type == RequestType::TXN_GET) {
          log->info("storage request getting txn id {} key {}", txn_id, key);
          if (stored_key_map.find(key) == stored_key_map.end()) {
            tp->set_error(AnnaError::KEY_DNE);
          } else {
            AnnaError error = AnnaError::NO_ERROR;
            auto res = process_txn_get(txn_id, key, error, serializer,
                                       stored_key_map);
            tp->set_payload(res);
            tp->set_error(error);
          }
        } else if (request_type == RequestType::TXN_PUT) {
          log->info("storage request putting txn id {} key {}", txn_id, key);
          AnnaError error = AnnaError::NO_ERROR;
          process_txn_put(txn_id, key, payload, error, is_primary, serializer, 
                          stored_key_map);
          tp->set_error(error);

          local_changeset.insert(key);
        } else if (request_type == RequestType::PREPARE_TXN) {
          if (stored_key_map.find(key) == stored_key_map.end()) {
            tp->set_error(AnnaError::KEY_DNE);
          } else {
            // check lock is still held; nothing actually done here for 2PL
            AnnaError error = AnnaError::NO_ERROR;
            process_txn_prepare(txn_id, key, error, serializer, stored_key_map);
            tp->set_error(error);

            if (error != AnnaError::NO_ERROR) {
              return;
            }

            // send replication / log requests
            ServerThreadList key_threads = kHashRingUtil->get_responsible_threads(
                wt.replication_response_connect_address(), request_type, txn_id, 
                key, is_metadata(key), 
                global_hash_rings, local_hash_rings, key_replication_map, 
                pushers, {Tier::LOG}, succeed, seed, log);

            // send request to log if possible
            if (key_threads.size() > 0) {
              kHashRingUtil->issue_log_request(
                wt.request_response_connect_address(), request_type, txn_id,
                key, payload, key_threads[0], pushers); // TODO(@accheng): how should we choose thread?
            }

            pending_requests[key].push_back( 
              PendingTxnRequest(request_type, txn_id, key, payload,
                                response_address, response_id));
          }
        } else if (request_type == RequestType::COMMIT_TXN) {
          // release lock
          AnnaError error = AnnaError::NO_ERROR;
          process_txn_commit(txn_id, key, error, serializer, stored_key_map);

          tp->set_error(error);

          if (error != AnnaError::NO_ERROR) {
            return;
          }

          // log commit
          ServerThreadList key_threads = kHashRingUtil->get_responsible_threads(
                wt.replication_response_connect_address(), request_type, txn_id, 
                key, is_metadata(key), 
                global_hash_rings, local_hash_rings, key_replication_map, 
                pushers, {Tier::LOG}, succeed, seed, log);

          // send request to log if possible
          if (key_threads.size() > 0) {
            kHashRingUtil->issue_log_request(
              wt.request_response_connect_address(), request_type, txn_id,
              key, payload, key_threads[0], pushers); // TODO(@accheng): how should we choose thread?
          }

          pending_requests[key].push_back( 
              PendingTxnRequest(request_type, txn_id, key, payload, "", /* response_address */
                                response_id));
        } else {
          log->error("Unknown request type {} in user request handler.",
                     request_type);
        }

        if (tuple.address_cache_size() > 0 &&
            tuple.address_cache_size() != threads.size()) {
          tp->set_invalidate(true);
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

  if (response.tuples_size() > 0 && request.response_address() != "" &&
      request_type != RequestType::PREPARE_TXN) { // need to wait for operation to be saved durably
    string serialized_response;
    response.SerializeToString(&serialized_response);
    kZmqUtil->send_string(serialized_response,
                          &pushers[request.response_address()]);
  }
}
