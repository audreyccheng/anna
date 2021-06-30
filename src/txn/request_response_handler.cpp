
#include "txn/txn_handlers.hpp"

void request_response_handler(
    unsigned &seed, unsigned &access_count, logger log, string &serialized,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<Key, vector<PendingTxnRequest>> &pending_requests,
    // map<Key, vector<PendingGossip>> &pending_gossip, // TODO(@accheng): update
    map<Key, std::multiset<TimePoint>> &key_access_tracker,
    map<Key, TxnKeyProperty> &stored_key_map,
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, TxnSerializer *txn_serializer, BaseSerializer *base_serializer, 
    LogSerializer *log_serializer, SocketCache &pushers) {
  log->info("Received request_response in handler");
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
  string payload = tuple.payload();

  AnnaError error = tuple.error();

  bool succeed;

  log->info("Received request_response key {} tuple_key {} type {}", key, tuple_key, response.type());
  if (pending_requests.find(key) != pending_requests.end()) {
    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
        wt.replication_response_connect_address(), response.type(), 
        response.txn_id(), key, is_metadata(key),
        global_hash_rings, local_hash_rings, key_replication_map, pushers,
        kSelfTierIdVector, succeed, seed, log);

    string suc = "false";
    if (succeed) {
      suc = "true";
    }
    log->info("request_response request getting threads success: {}", suc);

    if (succeed) {
      bool responsible =
          std::find(threads.begin(), threads.end(), wt) != threads.end();

      suc = "false";
      if (responsible) {
        suc = "true";
      }
      log->info("request_response request getting threads responsible: {}", suc);
      log->info("request_response pending_requests[key] {} size {} ", key, pending_requests[key].size());

      vector<unsigned> indices; // get requests with this tuple_key
      RequestTypeMap request_map; // map request types of this transaction
      for (unsigned i = 0; i < pending_requests[key].size(); ++i) {
        auto request = pending_requests[key][i];
        log->info("iterating request type {} key {}", request.type_, request.key_);
        if (request.key_ == tuple_key && request.type_ == response.type()) {
          log->info("found index {}", i);
          indices.push_back(i);
        }
        if (request_map.find(request.type_) == request_map.end()) {
          vector<unsigned> vec{ i };
          request_map[request.type_] = vec;
        } else {
          request_map[request.type_].push_back(i);
        }
      }
      log->info("made request map");

      unsigned erase_count = 0; // update index as requests are removed
      for (const unsigned &index : indices) {
        auto request = pending_requests[key].at(index - erase_count);
        auto now = std::chrono::system_clock::now();

        if (!responsible && request.addr_ != "") {
          log->info("Req_resp_handler if 1");
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
          log->info("Req_resp_handler if 2");
          // TODO(@accheng): only storage COMMIT_TXN?
          if (kSelfTier == Tier::MEMORY || kSelfTier == Tier::DISK &&
              request.type_ == RequestType::COMMIT_TXN) {

          } else {
            log->error("Received a request with no response address.");
          }
        } else if (responsible && request.addr_ != "") {
          log->info("Req_resp_handler if 3");
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
          	auto serializer = txn_serializer;
          	// handle response from storage tier
          	if (request.type_ == RequestType::TXN_GET || request.type_ == RequestType::TXN_PUT) {
              rep_response.set_txn_id(request.txn_id_);
          	  if (tuple.error() != AnnaError::NO_ERROR) {
                  // TODO(@accheng): need to abort txn

              }
              tp->set_key(tuple_key);
              tp->set_error(error);
              tp->set_payload(tuple.payload());
            } else if (request.type_ == RequestType::PREPARE_TXN) {
              // rep_response.set_txn_id(request.txn_id_);
              send_response = false;

              // check if any PREPARE_TXNs are still pending
              // if not, move onto commit phase
              if (request_map[request.type_].size() == 1) {
                log->info("req_resp only 1 prepare key {} txn_id {}", tuple_key, key);
                AnnaError error = AnnaError::NO_ERROR;
                auto ops = process_get_ops(key, error, serializer, stored_key_map);
                log->info("req_resp process_get_ops size {}", ops.size());
                tp->set_error(error);
                // TODO(@accheng): should txn abort if there is an error here?
                if (error != AnnaError::NO_ERROR) {
                  log->error("Unable to commit transaction");
                 // find COMMIT_TXN request from client and send response back
                } else if (request_map.find(RequestType::COMMIT_TXN) == request_map.end() ||
                           request_map[RequestType::COMMIT_TXN].size() != 1) {
                  log->error("Unable to find client request to commit");
                } else {
                  log->info("req_resp commit in map size {}", request_map[RequestType::COMMIT_TXN].size());
                  TxnResponse commit_response;
                  commit_response.set_type(RequestType::COMMIT_TXN);
                  commit_response.set_txn_id(key);
                  commit_response.set_tier(get_anna_tier_from_tier(kSelfTier));
                  TxnKeyTuple *commit_tp = commit_response.add_tuples();
                  unsigned commit_index = request_map[RequestType::COMMIT_TXN][0] - erase_count; // TODO(@accheng): should only ever be 1?
                  Address commit_response_addr = pending_requests[key][commit_index].addr_;
                  commit_response.set_response_id(pending_requests[key][commit_index].response_id_);

                  bool abort_txn = false;
                  vector<ServerThreadList> all_key_threads;
                  for (const Operation &op: ops) {
                    auto op_key = op.get_key();
                    auto op_payload = op.get_value();
                    ServerThreadList key_threads = {};

                    for (const Tier &tier : kStorageTiers) {
                      key_threads = kHashRingUtil->get_responsible_threads(
                          wt.replication_response_connect_address(), request.type_, 
                          response.txn_id(), op_key, is_metadata(op_key), 
                          global_hash_rings, local_hash_rings, key_replication_map, 
                          pushers, {tier}, succeed, seed, log);
                      if (key_threads.size() > 0) {
                        break;
                      }
                      log->info("Getting threads for storage tiers for key {} value {} tier {} key_threads size {} suc {}", 
                        op_key, op_payload, tier, key_threads.size(), succeed);

                      if (!succeed) { // this means we don't have the replication factor for
                                      // the key
                        // TODO(@accheng): should we abort here?
                        abort_txn = true;
                        log->error("Unable to find key to commit");
                        break;
                      }
                    }

                    log->info("user_txn_request ops size {} abort_txn {}", ops.size(), abort_txn);
                    if (abort_txn) {
                      break;
                    } else {
                      all_key_threads.push_back(key_threads);
                  	}
                  }

                  if (!abort_txn) {
                    commit_response.set_error(AnnaError::NO_ERROR);
                    commit_tp->set_key(tuple_key);
                    commit_tp->set_error(AnnaError::NO_ERROR);

                    // send COMMIT_TXN requests to storage tiers
                    for (unsigned i = 0; i < ops.size(); i++) {
                	    auto op_key = ops[i].get_key();
                  	  auto op_payload = ops[i].get_value();

                      kHashRingUtil->issue_storage_request(
                        wt.request_response_connect_address(), RequestType::COMMIT_TXN, key, 
                        op_key, op_payload, all_key_threads[i][0], pushers); // TODO(@accheng): how should we choose thread?
                      log->info("req_resp sending storage request  txn_id {} key {}", key, op_key);

                      pending_requests[key].push_back(
                          PendingTxnRequest(RequestType::COMMIT_TXN, key, op_key, op_payload,
                                            request.addr_, request.response_id_)); // TODO(@accheng): UPDATE
                    }
                  } else {
                    commit_response.set_error(AnnaError::FAILED_OP);
                    commit_tp->set_key(tuple_key);
                    commit_tp->set_error(AnnaError::FAILED_OP);
                  }

                  string serialized_commit_response;
                  commit_response.SerializeToString(&serialized_commit_response);
                  kZmqUtil->send_string(serialized_commit_response, &pushers[commit_response_addr]);


                  // erase the client COMMIT_TXN pending request
                  pending_requests[key].erase(pending_requests[key].begin() + commit_index);
    		          request_map[RequestType::COMMIT_TXN].erase(request_map[RequestType::COMMIT_TXN].begin());
    		          if (commit_index + erase_count < index) {
    		          	erase_count++;
    		          }
                }
              }
              log->info("request_response pending_requests[key] {} size {} ", key, pending_requests[key].size());
          	} else if (request.type_ == RequestType::COMMIT_TXN) {
              // response sent to client after prepare phase
              send_response = false;
              // check if any commits pending, otherwise finalize commit
              if (request_map[request.type_].size() == 1) {
                process_commit_txn(key, error, serializer, stored_key_map);
                log->info("request_response process_commit_txn key {} error {}", key, error);
                auto temp_val = base_serializer->reveal_element(key, error);
                log->info("***** request_response now holds at key {} value {} error {}", key, temp_val, error);


                // TODO(@accheng): Log commit on coordinator
                /* NO LOG */
              }
            } else {
              log->error("Wrong request type to transactional tier");
              continue;
            }

          } else if (kSelfTier == Tier::MEMORY || kSelfTier == Tier::DISK) {
          	auto serializer = base_serializer;
            rep_response.set_txn_id(request.txn_id_);

            // response from log tier
            if (request.type_ == RequestType::PREPARE_TXN || 
            	request.type_ == RequestType::COMMIT_TXN) {
              if (key_tier != Tier::LOG) {
                log->error("Wrong request to storage tier for prepare / commit");
                continue;
              } else {
              	tp->set_key(tuple_key);
                tp->set_error(error);
                tp->set_payload(tuple.payload());
              }
            } else {
           	  log->error("Wrong request type to storage tier");
           	  continue;
            }
          } else if (kSelfTier == Tier::LOG) {
          	auto serializer = log_serializer;
            rep_response.set_txn_id(request.txn_id_);

            /* LOG tier */
            if (request.type_ == RequestType::PREPARE_TXN || 
                request.type_ == RequestType::COMMIT_TXN) {
              process_log(key, tuple_key, payload, error, serializer); // TODO(@accheng): update
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
  	}  else {
      log->error(
          "Missing key replication factor in process pending request routine.");
    }

    // pending_requests.erase(key);
    if (pending_requests[key].size() == 0) {
      pending_requests.erase(key);
    }
  }
}