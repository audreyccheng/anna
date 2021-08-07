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

void replication_response_handler(
    unsigned &seed, unsigned &access_count, logger log, string &serialized,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<Key, vector<PendingTxnRequest>> &pending_requests,
    // map<Key, vector<PendingGossip>> &pending_gossip, // TODO(@accheng): update
    map<Key, std::multiset<TimePoint>> &key_access_tracker,
    map<Key, TxnKeyProperty> &stored_key_map,
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, TxnSerializer *txn_serializer, BaseSerializer *base_serializer, 
    LogSerializer *log_serializer, SocketCache &pushers) {
  // log->info("Received replication_response request");
  TxnResponse response;
  response.ParseFromString(serialized);

  // we assume tuple 0 because there should only be one tuple responding to a
  // replication factor request
  TxnKeyTuple tuple = response.tuples(0);
  // Key key = get_key_from_metadata(tuple.key());
  Key key = response.txn_id();
  Key tuple_key = get_txn_key_from_metadata(tuple.key());
  if (kSelfTier != Tier::TXN || key == "") { // TODO(@accheng): is this correct? 
    key = tuple_key;
  }
  Tier key_tier = get_tier_from_anna_tier(response.tier());
  // string payload = tuple.payload();

  AnnaError error = tuple.error();

  bool txn_tier = false;
  if (response.type() == RequestType::START_TXN) {
    txn_tier = true;
  }

  // log->info("Received replication_response request type {} txn_id {} key {} tier {}",
    response.type(), response.txn_id(), key, key_tier);

  if (error == AnnaError::NO_ERROR) {
    // TODO(@accheng): update; this is called in for replication_change only
    // LWWValue lww_value;
    // lww_value.ParseFromString(tuple.payload());
    ReplicationFactor rep_data;
    rep_data.ParseFromString(tuple.payload());

    for (const auto &global : rep_data.global()) {
      key_replication_map[key].global_replication_[global.tier()] =
          global.value();
    }

    for (const auto &local : rep_data.local()) {
      key_replication_map[key].local_replication_[local.tier()] = local.value();
    }
  } else if (error == AnnaError::KEY_DNE || error == AnnaError::TXN_DNE) {
    // log->info("replication_response request KEY_DNE");
    // KEY_DNE means that the receiving thread was responsible for the metadata
    // but didn't have any values stored -- we use the default rep factor
    // init_tier_replication(key_replication_map, tuple_key, key_tier);
    init_replication(key_replication_map, tuple_key);
    // log->info("replication_response init_tier_replication tier {} key {}", key_tier, tuple_key);
    // log->info("replication_response init_replication key {}", tuple_key);
  } else if (error == AnnaError::WRONG_THREAD) {
    // this means that the node that received the rep factor request was not
    // responsible for that metadata
    auto respond_address = wt.replication_response_connect_address();
    kHashRingUtil->issue_replication_factor_request( // TODO(@accheng): is this ok for non-txn_id keys?
        respond_address, response.type(), response.txn_id(),
        key, key_tier, global_hash_rings[key_tier],
        local_hash_rings[key_tier], pushers, seed, log);
    return;
  } else {
    log->error("Unexpected error type {} in replication factor response.",
               error);
    return;
  }

  bool succeed;

  if (pending_requests.find(key) != pending_requests.end()) {
    // log->info("replication_response request found key {}", key);
    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
        wt.replication_response_connect_address(), response.type(), 
        response.txn_id(), key, is_metadata(key),
        global_hash_rings, local_hash_rings, key_replication_map, pushers,
        kSelfTierIdVector, succeed, seed, log);

    string suc = "false";
    if (succeed) {
      suc = "true";
    }
    // log->info("replication_response request getting threads success: {}", suc);

    if (succeed) {
      bool responsible =
          std::find(threads.begin(), threads.end(), wt) != threads.end();

      suc = "false";
      if (responsible) {
        suc = "true";
      }
      // log->info("replication_response request getting threads responsible: {}", suc);

      vector<unsigned> indices; // get requests with this tuple_key
      RequestTypeMap request_map; // map request types of this transaction
      for (unsigned i = 0; i < pending_requests[key].size(); ++i) {
        auto request = pending_requests[key][i];
        if (request.key_ == tuple_key && request.type_ == response.type()) {
          indices.push_back(i);
        }
        if (request_map.find(request.type_) == request_map.end()) {
          vector<unsigned> vec{ i };
          request_map[request.type_] = vec;
        } else {
          request_map[request.type_].push_back(i);
        }
      }

      // for (const PendingRequest &request : pending_requests[key]) { // TODO(@accheng): update
      // for (int i = 0; i < pending_requests[key].size(); ++i) {
      // int i = 0;
      // for (auto it = pending_requests[key].begin(); it != pending_requests[key].end(); ) {

      unsigned erase_count = 0; // update index as requests are removed
      for (const unsigned &index : indices) {
        auto request = pending_requests[key].at(index - erase_count);
        auto now = std::chrono::system_clock::now();

        if (!responsible && request.addr_ != "") {
          // log->info("Rep_resp_handler if 1");
          TxnResponse response;

          response.set_type(request.type_);
          response.set_txn_id(request.txn_id_);
          response.set_tier(get_anna_tier_from_tier(kSelfTier));

          if (request.response_id_ != "") {
            response.set_response_id(request.response_id_);
          }

          TxnKeyTuple *tp = response.add_tuples();
          tp->set_key(tuple_key);
          tp->set_payload(request.payload_);
          tp->set_error(AnnaError::WRONG_THREAD);

          string serialized_response;
          response.SerializeToString(&serialized_response);
          kZmqUtil->send_string(serialized_response, &pushers[request.addr_]);
        } else if (responsible && request.addr_ == "") {
          // log->info("Rep_resp_handler if 2");
          // TODO(@accheng): only storage COMMIT_TXN?
          if (kSelfTier == Tier::MEMORY || kSelfTier == Tier::DISK &&
              request.type_ == RequestType::COMMIT_TXN) {

          } else {
            log->error("Received a request with no response address.");
          }
        } else if (responsible && request.addr_ != "") {
          // log->info("Rep_resp_handler if 3");
          bool send_response = true;

          TxnResponse rep_response;
          rep_response.set_type(request.type_);
          rep_response.set_tier(get_anna_tier_from_tier(kSelfTier));

          if (request.response_id_ != "") {
            rep_response.set_response_id(request.response_id_);
          }

          TxnKeyTuple *tp = rep_response.add_tuples();
          tp->set_key(tuple_key);

          /* TXN tier */
          if (kSelfTier == Tier::TXN) {
            // auto stored_txn_map = stored_key_map; // TODO(@accheng): update
            auto serializer = txn_serializer;

            if (request.type_ == RequestType::START_TXN) {
              auto txn_id = process_start_txn(tuple_key, serializer, stored_key_map);
              rep_response.set_txn_id(txn_id);

              // need to add txn_id to key_rep_map
              init_tier_replication(key_replication_map, txn_id, kSelfTier);
              init_tier_replication(key_replication_map, txn_id, Tier::LOG);

              // TODO(@accheng): erase from pending requests
            } else if (request.type_ == RequestType::TXN_GET || 
                       request.type_ == RequestType::TXN_PUT) {
              rep_response.set_txn_id(request.txn_id_);

              // if this is from txn tier, this request needs to sent to storage tier
              // otherwise, respond to client
              // if (key_tier == Tier::TXN) {
              if (stored_key_map.find(key) == stored_key_map.end()) {
                tp->set_error(AnnaError::TXN_DNE);
              } else {
                ServerThreadList key_threads = {};

                for (const Tier &tier : kStorageTiers) {
                  key_threads = kHashRingUtil->get_responsible_threads(
                      wt.replication_response_connect_address(), request.type_,
                      response.txn_id(), tuple_key, is_metadata(tuple_key), 
                      global_hash_rings, local_hash_rings, key_replication_map, 
                      pushers, {tier}, succeed, seed, log);

                  string suc = "false";
                  if (succeed) {
                    suc = "true";
                  }
                  // log->info("replication_response request getting threads for tuple_key {} success {} key_threads size {}",
                  //   tuple_key, suc, key_threads.size());

                  if (key_threads.size() > 0) {
                    break;
                  }

                  if (!succeed) { // this means we don't have the replication factor for
                                  // the key and we can't replicate it
                    tp->set_error(AnnaError::KEY_DNE);
                  }
                }

                // TODO(@accheng): should just be one request?
                // send request to storage tier
                if (key_threads.size() > 0) {
                  // log->info("replication_response issuing storage request");
                  kHashRingUtil->issue_storage_request(
                    wt.request_response_connect_address(), request.type_, key, tuple_key, 
                    request.payload_, key_threads[0], pushers); // TODO(@accheng): how should we choose thread?

                  // add to pending request
                  // pending_requests[key].push_back(
                  //         PendingTxnRequest(request.type_, key, tuple_key, payload,
                  //                           request.addr_, request.response_id_)); // TODO(@accheng): UPDATE
                  // it++;
                  // i++;
                  continue;
                } else {
                  // TODO(@accheng): erase from pending requests
                }
              }
              // } else { // store info from storage tier
                
              // }
            } else if (request.type_ == RequestType::PREPARE_TXN) {
              // TODO(@accheng): this shouldn't happen?
              log->error(
                "Prepare request can not be fulfilled in replication_response_handler.");
              continue;
            } else if (request.type_ == RequestType::COMMIT_TXN) {
              // // response sent to client after prepare phase
              // send_response = false;
              // // check if any commits pending, otherwise finalize commit
              // if (request_map[request.type_].size() == 1) {
              //   process_commit_txn(key, error, serializer, stored_key_map);
              // }

              // TODO(@accheng): this shouldn't happen?
              log->error(
                "Commit request ccan not be fulfilled in replication_response_handler.");
              continue;
            }
          } else if (kSelfTier == Tier::MEMORY || kSelfTier == Tier::DISK) {
            auto serializer = base_serializer;
            rep_response.set_txn_id(request.txn_id_);

            bool is_primary = true; // TODO(@accheng): update

            /* STORAGE tier */
            if (request.type_ == RequestType::TXN_GET) {
              if (stored_key_map.find(key) == stored_key_map.end()) {
                tp->set_error(AnnaError::KEY_DNE);
                stored_key_map[key].lock_ = 1;
                AnnaError notify_error = AnnaError::NO_ERROR;
                base_serializer->notify_dne_get(response.txn_id(), key, notify_error);
              } else {
                AnnaError error = AnnaError::NO_ERROR;
                auto res = process_txn_get(request.txn_id_, key, error,
                                           serializer, 
                                           stored_key_map);
                // log->info("replication_response process_txn_get payload {} error {}", res, error);
                tp->set_payload(res);
                tp->set_error(error);
              }
            } else if (request.type_ == RequestType::TXN_PUT) {
              AnnaError error = AnnaError::NO_ERROR;
              process_txn_put(request.txn_id_, key, request.payload_, error,
                              is_primary, serializer, stored_key_map);
              // log->info("replication_response process_txn_put payload{} error {}", request.payload_, error);
              auto temp_val = base_serializer->reveal_temp_element(key, error);
              // log->info("***** storage request now holds at key {} temp_value {} error {}", key, temp_val, error);
              tp->set_error(error);

              local_changeset.insert(key);
            } else if (request.type_ == RequestType::PREPARE_TXN) {
              // if (key_tier == kSelfTier) {
              if (stored_key_map.find(key) == stored_key_map.end()) {
                tp->set_error(AnnaError::KEY_DNE);
              } else {
                // check lock is still held; nothing actually done here for 2PL
                AnnaError error = AnnaError::NO_ERROR;
                process_txn_prepare(key, tuple_key, error, 
                                    serializer, stored_key_map);
                tp->set_error(error);

                /* NO LOG */
                // send replication / log requests
                ServerThreadList key_threads = kHashRingUtil->get_responsible_threads(
                    wt.replication_response_connect_address(), request.type_, 
                    response.txn_id(), key, is_metadata(key), 
                    global_hash_rings, local_hash_rings, key_replication_map, 
                    pushers, {Tier::LOG}, succeed, seed, log);

                // send request to log if possible
                if (key_threads.size() > 0) {
                  kHashRingUtil->issue_log_request(
                    wt.request_response_connect_address(), request.type_, request.txn_id_,
                    key, request.payload_, key_threads[0], pushers); // TODO(@accheng): how should we choose thread?
                }

                // pending_requests[key].push_back( 
                //   PendingTxnRequest(request.type_, key, tuple_key, payload,
                //                     request.addr_, request.response_id_)); // TODO(@accheng): UPDATE
                continue;
                /* NO LOG */
              }
            } else if (request.type_ == RequestType::COMMIT_TXN) { // TODO(@accheng): this should never happen?
              if (stored_key_map.find(key) == stored_key_map.end()) {
                tp->set_error(AnnaError::KEY_DNE);
              } else {
                AnnaError error = AnnaError::NO_ERROR;
                process_txn_commit(request.txn_id_, key, error, serializer, stored_key_map);

                tp->set_error(error);

                /* NO LOG */
                // log commit
                ServerThreadList key_threads = kHashRingUtil->get_responsible_threads(
                      wt.replication_response_connect_address(), request.type_, 
                      response.txn_id(), key, is_metadata(key), 
                      global_hash_rings, local_hash_rings, key_replication_map, 
                      pushers, {Tier::LOG}, succeed, seed, log);

                // send request to log if possible
                if (key_threads.size() > 0) {
                  kHashRingUtil->issue_log_request(
                    wt.request_response_connect_address(), request.type_, 
                    request.txn_id_, key, request.payload_, key_threads[0], pushers); // TODO(@accheng): how should we choose thread?
                }

                continue;
                /* NO LOG */
              }
            }
          } else if (kSelfTier == Tier::LOG) { 
            auto serializer = log_serializer;
            rep_response.set_txn_id(request.txn_id_);

            /* LOG tier */
            if (request.type_ == RequestType::PREPARE_TXN || 
                request.type_ == RequestType::COMMIT_TXN) {
              // log->info("Logged for txn {}, type {} ", request.txn_id_, request.type_);
              process_log(request.txn_id_, key, request.payload_, error, serializer); // TODO(@accheng): update
              tp->set_error(error);
            } else {
              log->error("Unknown request type {} in user request handler.",
                         request.type_);
            }
          }

          // TODO(@accheng): should this be here?
          // erase this request from pending_requests and requests_map
          // it = pending_requests[key].erase(it);
          pending_requests[key].erase(pending_requests[key].begin() + index - erase_count);
          auto vit = find(request_map[request.type_].begin(),
                          request_map[request.type_].end(), index - erase_count);
          if (vit != request_map[request.type_].end()) {
            request_map[request.type_].erase(vit);
          }
          erase_count++;
          // i++;

          key_access_tracker[key].insert(now);
          access_count += 1;

          if (send_response) {
            string serialized_response;
            rep_response.SerializeToString(&serialized_response);
            kZmqUtil->send_string(serialized_response, &pushers[request.addr_]);
          }

        }
      }
    } else {
      log->error(
          "Missing key replication factor in process pending request routine.");
    }

    // pending_requests.erase(key);
    if (pending_requests[key].size() == 0) {
      pending_requests.erase(key);
    }
  }

  // if (pending_gossip.find(key) != pending_gossip.end()) {
  //   ServerThreadList threads = kHashRingUtil->get_responsible_threads(
  //       wt.replication_response_connect_address(), key, is_metadata(key),
  //       global_hash_rings, local_hash_rings, key_replication_map, pushers,
  //       kSelfTierIdVector, succeed, seed);

  //   if (succeed) {
  //     if (std::find(threads.begin(), threads.end(), wt) != threads.end()) {
  //       for (const PendingGossip &gossip : pending_gossip[key]) {
  //         if (stored_key_map.find(key) != stored_key_map.end() &&
  //             stored_key_map[key].type_ != LatticeType::NONE &&
  //             stored_key_map[key].type_ != gossip.lattice_type_) {
  //           log->error("Lattice type mismatch for key {}: {} from query but {} "
  //                      "expected.",
  //                      key, LatticeType_Name(gossip.lattice_type_),
  //                      LatticeType_Name(stored_key_map[key].type_));
  //         } else {
  //           process_put(key, gossip.lattice_type_, gossip.payload_,
  //                       serializers[gossip.lattice_type_], stored_key_map);
  //         }
  //       }
  //     } else {
  //       map<Address, KeyRequest> gossip_map;

  //       // forward the gossip
  //       for (const ServerThread &thread : threads) {
  //         gossip_map[thread.gossip_connect_address()].set_type(
  //             RequestType::PUT);

  //         for (const PendingGossip &gossip : pending_gossip[key]) {
  //           prepare_put_tuple(gossip_map[thread.gossip_connect_address()], key,
  //                             gossip.lattice_type_, gossip.payload_);
  //         }
  //       }

  //       // redirect gossip
  //       for (const auto &gossip_pair : gossip_map) {
  //         string serialized;
  //         gossip_pair.second.SerializeToString(&serialized);
  //         kZmqUtil->send_string(serialized, &pushers[gossip_pair.first]);
  //       }
  //     }
  //   } else {
  //     log->error(
  //         "Missing key replication factor in process pending gossip routine.");
  //   }

  //   pending_gossip.erase(key);
  // }
}
