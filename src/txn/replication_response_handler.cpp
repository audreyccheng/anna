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
  TxnResponse response;
  response.ParseFromString(serialized);

  // we assume tuple 0 because there should only be one tuple responding to a
  // replication factor request
  TxnKeyTuple tuple = response.tuples(0);
  // Key key = get_key_from_metadata(tuple.key());
  Key key = response.txn_id();
  Key tuple_key = get_key_from_metadata(tuple.key());
  if (kSelfTier != Tier::TXN) {
    key = tuple_key;
  }
  Tier key_tier = get_tier_from_anna_tier(response.tier());
  string payload = tuple.payload();

  AnnaError error = tuple.error();

  bool txn_tier = false;
  if (response.type() == RequestType::START_TXN) {
    txn_tier = true;
  }

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
  } else if (error == AnnaError::KEY_DNE) {
    // KEY_DNE means that the receiving thread was responsible for the metadata
    // but didn't have any values stored -- we use the default rep factor
    init_tier_replication(key_replication_map, key, kSelfTier);
  } else if (error == AnnaError::WRONG_THREAD) {
    // this means that the node that received the rep factor request was not
    // responsible for that metadata
    auto respond_address = wt.replication_response_connect_address();
    kHashRingUtil->issue_replication_factor_request( // TODO(@accheng): is this ok for non-txn_id keys?
        respond_address, key, key_tier, global_hash_rings[key_tier],
        local_hash_rings[key_tier], pushers, seed);
    return;
  } else {
    log->error("Unexpected error type {} in replication factor response.",
               error);
    return;
  }

  bool succeed;

  if (pending_requests.find(key) != pending_requests.end()) {
    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
        wt.replication_response_connect_address(), key, is_metadata(key),
        global_hash_rings, local_hash_rings, key_replication_map, pushers,
        kSelfTierIdVector, succeed, seed);

    if (succeed) {
      bool responsible =
          std::find(threads.begin(), threads.end(), wt) != threads.end();

      // map request types of this transaction
      RequestTypeMap request_map;
      for (unsigned i = 0; i < pending_requests[key].size(); ++i) {
        auto request = pending_requests[key][i];
        if (request_map.find(request.type_) == request_map.end()) {
          vector<unsigned> vec{ i };
          request_map[request.type_] = vec;
        } else {
          request_map[request.type_].push_back(i);
        }
      }

      // for (const PendingRequest &request : pending_requests[key]) { // TODO(@accheng): update
      // for (int i = 0; i < pending_requests[key].size(); ++i) {
      int i = 0;
      for (auto it = pending_requests[key].begin(); it != pending_requests[key].end(); ) {
        auto request = pending_requests[key][i];
        auto now = std::chrono::system_clock::now();

        if (!responsible && request.addr_ != "") {
          KeyResponse response;

          response.set_type(request.type_);

          if (request.response_id_ != "") {
            response.set_response_id(request.response_id_);
          }

          KeyTuple *tp = response.add_tuples();
          tp->set_key(tuple_key);
          tp->set_error(AnnaError::WRONG_THREAD);

          string serialized_response;
          response.SerializeToString(&serialized_response);
          kZmqUtil->send_string(serialized_response, &pushers[request.addr_]);
        } else if (responsible && request.addr_ == "") {
          // // only put requests should fall into this category
          // if (request.type_ == RequestType::PUT) {
          //   if (request.lattice_type_ == LatticeType::NONE) {
          //     log->error("PUT request missing lattice type.");
          //   } else if (stored_key_map.find(key) != stored_key_map.end() &&
          //              stored_key_map[key].type_ != LatticeType::NONE &&
          //              stored_key_map[key].type_ != request.lattice_type_) {

          //     log->error(
          //         "Lattice type mismatch for key {}: query is {} but we expect "
          //         "{}.",
          //         key, LatticeType_Name(request.lattice_type_),
          //         LatticeType_Name(stored_key_map[key].type_));
          //   } else {
          //     process_put(key, request.lattice_type_, request.payload_,
          //                 serializers[request.lattice_type_], stored_key_map);
          //     key_access_tracker[key].insert(now);

          //     access_count += 1;
          //     local_changeset.insert(key);
          //   }
          // } else {
          //   log->error("Received a GET request with no response address.");
          // }
          log->error("Received a request with no response address.");
        } else if (responsible && request.addr_ != "") {
          TxnResponse rep_response;

          rep_response.set_type(request.type_);

          if (request.response_id_ != "") {
            rep_response.set_response_id(request.response_id_);
          }

          TxnKeyTuple *tp = rep_response.add_tuples();
          tp->set_key(tuple_key);


          /* TXN tier */
          if (kSelfTier == Tier::TXN) {
            auto stored_txn_map = stored_key_map; // TODO(@accheng): update
            auto serializer = txn_serializer;

            if (request.type_ == RequestType::START_TXN) {
              auto txn_id = process_start_txn(tuple_key, serializer, stored_txn_map);
              rep_response.set_txn_id(txn_id);

              // need to add txn_id to key_rep_map
              init_tier_replication(key_replication_map, txn_id, kSelfTier);

              // TODO(@accheng): erase from pending requests

            } else if (request.type_ == RequestType::TXN_GET) {
              // if this is from txn tier, this request needs to sent to storage tier
              // otherwise, respond to client
              if (key_tier == Tier::TXN) {
                if (stored_txn_map.find(key) == stored_txn_map.end()) {
                  tp->set_error(AnnaError::TXN_DNE);
                }

                ServerThreadList key_threads = {};

                for (const Tier &tier : kStorageTiers) {
                  key_threads = kHashRingUtil->get_responsible_threads(
                      wt.replication_response_connect_address(), tuple_key, is_metadata(tuple_key), 
                      global_hash_rings, local_hash_rings, key_replication_map, 
                      pushers, {tier}, succeed, seed);
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
                  kHashRingUtil->issue_storage_request(
                    wt.replication_response_connect_address(), request.type_, key, tuple_key, 
                    payload, key_threads[0], pushers); // TODO(@accheng): how should we choose thread?

                  // add to pending request
                  pending_requests[key].push_back(
                          PendingTxnRequest(request.type_, key, tuple_key, payload,
                                            request.addr_, request.response_id_)); // TODO(@accheng): UPDATE
                } else {
                  // TODO(@accheng): erase from pending requests
                }
                
              } else { // store info from storage tier
                tp->set_key(tuple_key);
                tp->set_error(AnnaError::NO_ERROR);
                tp->set_payload(tuple.payload());
              }
            } else if (request.type_ == RequestType::PREPARE_TXN) {
              // check if any PREPARE_TXNs are still pending
              // if not, move onto commit phase
              if (request_map[request.type_].size() == 1) {
                AnnaError error = AnnaError::NO_ERROR;
                auto ops = process_get_ops(key, error, serializer, stored_txn_map);
                tp->set_error(error);
                // TODO(@accheng): should txn abort if there is an error here?
                if (error != AnnaError::NO_ERROR) {
                  log->error("Unable to commit transaction");
                 // find COMMIT_TXN request from client and send response back
                } else if (request_map.find(RequestType::COMMIT_TXN) == request_map.end() ||
                           request_map[RequestType::COMMIT_TXN].size() != 1) {
                  log->error("Unable to find client request to commit");
                } else {
                  TxnResponse commit_response;
                  commit_response.set_type(RequestType::COMMIT_TXN);
                  commit_response.set_txn_id(key);
                  commit_response.set_tier(get_anna_tier_from_tier(kSelfTier));
                  TxnKeyTuple *commit_tp = commit_response.add_tuples();
                  unsigned commit_index = request_map[RequestType::COMMIT_TXN][0];
                  string commit_response_addr = pending_requests[key][commit_index].response_id_; // TODO(@accheng): should only be 1?

                  bool abort_txn;
                  for (const Operation &op: ops) {
                    auto op_key = op.get_key();
                    auto op_payload = op.get_value();
                    ServerThreadList key_threads = {};

                    for (const Tier &tier : kStorageTiers) {
                      key_threads = kHashRingUtil->get_responsible_threads(
                          wt.replication_response_connect_address(), tuple_key, is_metadata(tuple_key), 
                          global_hash_rings, local_hash_rings, key_replication_map, 
                          pushers, {tier}, succeed, seed);
                      if (key_threads.size() > 0) {
                        break;
                      }

                      if (!succeed) { // this means we don't have the replication factor for
                                      // the key
                        // TODO(@accheng): should we abort here?
                        abort_txn = true;
                        log->error("Unable to find key to commit");
                        break;
                      }
                    }

                    if (abort_txn) {
                      tp->set_error(AnnaError::FAILED_OP); // TODO(@accheng): needed?
                      break;
                    } else {
                      // send prepare request to storage tier
                      kHashRingUtil->issue_storage_request(
                        wt.replication_response_connect_address(), RequestType::COMMIT_TXN, key, 
                        op_key, op_payload, key_threads[0], pushers); // TODO(@accheng): how should we choose thread?

                      pending_requests[key].push_back(
                          PendingTxnRequest(RequestType::COMMIT_TXN, key, op_key, op_payload,
                                            request.addr_, request.response_id_)); // TODO(@accheng): UPDATE
                    }
                  }

                  if (!abort_txn) {
                    commit_response.set_error(AnnaError::NO_ERROR);
                    commit_tp->set_key(tuple_key);
                    commit_tp->set_error(AnnaError::NO_ERROR);
                  } else {
                    commit_response.set_error(AnnaError::FAILED_OP);
                    commit_tp->set_key(tuple_key);
                    commit_tp->set_error(AnnaError::FAILED_OP);
                  }

                  string serialized_commit_response;
                  commit_response.SerializeToString(&serialized_commit_response);
                  kZmqUtil->send_string(serialized_commit_response, &pushers[commit_response_addr]);
                }
              } else if (request.type_ == RequestType::COMMIT_TXN) {
                // check if any commits pending, otherwise finalize commit
                if (request_map[request.type_].size() == 1) {
                  // process_commit_txn(txn_id, error, serializer);
                }
              }

          } else if (kSelfTier == Tier::MEMORY || kSelfTier == Tier::DISK) {
            auto serializer = base_serializer;

            /* STORAGE tier */
            if (request.type_ == RequestType::TXN_GET) {
              if (stored_key_map.find(key) == stored_key_map.end()) {
                tp->set_error(AnnaError::KEY_DNE);
              } else {
                AnnaError error = AnnaError::NO_ERROR;
                auto res = process_txn_get(key, tuple_key, error,
                                           serializer, 
                                           stored_key_map);
                tp->set_payload(res);
                tp->set_error(error);
              }
            } else if (request.type_ == RequestType::TXN_PUT) {
              AnnaError error = AnnaError::NO_ERROR;
              process_txn_put(key, tuple_key, payload, error,
                              serializer, stored_key_map);
              tp->set_error(error);

              local_changeset.insert(key);
            } else if (request.type_ == RequestType::PREPARE_TXN) {
              if (stored_key_map.find(key) == stored_key_map.end()) {
                tp->set_error(AnnaError::KEY_DNE);
              } else {
                // check lock is still held; nothin actually done here for 2PL
                AnnaError error = AnnaError::NO_ERROR;
                process_txn_prepare(key, tuple_key, error, 
                                    serializer, stored_key_map);
                tp->set_error(error);

                // send replication / log requests
                ServerThreadList key_threads = kHashRingUtil->get_responsible_threads(
                    wt.replication_response_connect_address(), tuple_key, is_metadata(tuple_key), 
                    global_hash_rings, local_hash_rings, key_replication_map, 
                    pushers, {Tier::LOG}, succeed, seed);

                // send request to log if possible
                if (key_threads.size() > 0) {
                  kHashRingUtil->issue_log_request(
                    wt.replication_response_connect_address(), request.type_, key,
                    tuple_key, payload, key_threads[0], pushers); // TODO(@accheng): how should we choose thread?
                }

                pending_requests[key].push_back( 
                  PendingTxnRequest(request.type_, key, tuple_key, payload,
                                    request.addr_, request.response_id_)); // TODO(@accheng): UPDATE
              }
            }

          } else if (kSelfTier == Tier::LOG) {
            auto serializer = log_serializer;
            /* LOG tier */
            if (request.type_ == RequestType::PREPARE_TXN || 
                request.type_ == RequestType::COMMIT_TXN) {
              process_log(key, tuple_key, payload, error, serializer); // TODO(@accheng): update
            } else {
              log->error("Unknown request type {} in user request handler.",
                         request.type_);
            }
          }

          // TODO(@accheng): should this be here?
          // erase this request from pending_requests and requests_map
          it = pending_requests[key].erase(it);
          auto vit = find(request_map[request.type_].begin(), request_map[request.type_].end(), i);
          if (vit != request_map[request.type_].end()) {
            request_map[request.type_].erase(vit);
          }
          i++;


          // if (request.type_ == RequestType::GET) {
          //   if (stored_key_map.find(key) == stored_key_map.end() ||
          //       stored_key_map[key].type_ == LatticeType::NONE) {
          //     tp->set_error(AnnaError::KEY_DNE);
          //   } else {
          //     auto res =
          //         process_get(key, serializers[stored_key_map[key].type_]);
          //     tp->set_lattice_type(stored_key_map[key].type_);
          //     tp->set_payload(res.first);
          //     tp->set_error(res.second);
          //   }
          // } else {
          //   if (request.lattice_type_ == LatticeType::NONE) {
          //     log->error("PUT request missing lattice type.");
          //   } else if (stored_key_map.find(key) != stored_key_map.end() &&
          //              stored_key_map[key].type_ != LatticeType::NONE &&
          //              stored_key_map[key].type_ != request.lattice_type_) {
          //     log->error(
          //         "Lattice type mismatch for key {}: {} from query but {} "
          //         "expected.",
          //         key, LatticeType_Name(request.lattice_type_),
          //         LatticeType_Name(stored_key_map[key].type_));
          //   } else {
          //     process_put(key, request.lattice_type_, request.payload_,
          //                 serializers[request.lattice_type_], stored_key_map);
          //     tp->set_lattice_type(request.lattice_type_);
          //     local_changeset.insert(key);
          //   }
          // }

          key_access_tracker[key].insert(now);
          access_count += 1;

          string serialized_response;
          response.SerializeToString(&serialized_response);
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
