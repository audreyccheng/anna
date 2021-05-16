#include "txn/txn_handlers.hpp"

void user_txn_request_handler(
    unsigned &access_count, unsigned &seed, string &serialized, logger log,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    // map<Key, vector<PendingRequest>> &pending_requests,
    map<string, vector<PendingTxnRequest>> &pending_requests, // <txn_id, <request_id, PendingTxnRequest>> 
    map<Key, std::multiset<TimePoint>> &key_access_tracker,
    map<Key, TxnKeyProperty> &stored_txn_map, // <txn_id, TxnKeyProperty>
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, TxnSerializer *serializer, SocketCache &pushers) {
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

  // TODO(@accheng): should only be one tuple?
  for (const auto &tuple : request.tuples()) {
    // first check if the thread is responsible for the key
    Key key = tuple.key();
    string payload = tuple.payload();

    log->info("Received user_txn_request of type {} for key {}", request_type, key);

    // Key key = txn_id;
    if (request_type == RequestType::START_TXN) {
      // a START_TXN request doesn't have a txn_id so we use client_id
      txn_id = key;
    }

    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
        wt.replication_response_connect_address(), request_type, 
        txn_id, txn_id, is_metadata(txn_id), 
        global_hash_rings, local_hash_rings, key_replication_map, 
        pushers, kSelfTierIdVector, succeed, seed, log);

    string suc = "false";
    if (succeed) {
      suc = "true";
    }
    log->info("User txn request getting threads success: {}", suc);

    if (succeed) {
      if (std::find(threads.begin(), threads.end(), wt) == threads.end()) {
        if (is_metadata(txn_id)) {
          // this means that this node is not responsible for this metadata key
          TxnKeyTuple *tp = response.add_tuples();

          tp->set_key(key);
          tp->set_error(AnnaError::WRONG_THREAD);
        } else {
          // if we don't know what threads are responsible, we issue a rep
          // factor request and make the request pending
          kHashRingUtil->issue_replication_factor_request(
              wt.replication_response_connect_address(), request_type, 
              txn_id, key, Tier::TXN, 
              global_hash_rings[Tier::TXN], local_hash_rings[Tier::TXN],
              pushers, seed, log);

          // since this is a new client request, key is client_id instead of txn_id
          pending_requests[txn_id].push_back( 
              PendingTxnRequest(request_type, txn_id, key, payload,
                                response_address, response_id));
        }
      } else { // if we know the responsible threads, we process the request
        TxnKeyTuple *tp = response.add_tuples();
        tp->set_key(key);

        // check if this is an replication factor request
        if (is_metadata(txn_id) && stored_txn_map.find(txn_id) == stored_txn_map.end()) {
          tp->set_error(AnnaError::KEY_DNE);
        } else if (request_type == RequestType::START_TXN) {
          // if this is a replication request, signal that this key doesn't yet exist
          // TODO(@accheng): update
          if (stored_txn_map.find(txn_id) == stored_txn_map.end() && is_metadata(txn_id)) { 
            tp->set_error(AnnaError::KEY_DNE);
          } else { // TODO(@accheng): update
            auto new_txn_id = process_start_txn(txn_id, serializer, stored_txn_map); // add txn_id to stored_txn_map
            response.set_txn_id(new_txn_id);

            // need to add txn_id to key_rep_map
            init_tier_replication(key_replication_map, txn_id, kSelfTier);

            // TODO(@accheng): make sure all relevant threads know that it is now responsbile for this txn_id
            // kHashRingUtil->issue_replication_factor_request( 
            //   wt.replication_response_connect_address(), txn_id, Tier::TXN, 
            //   global_hash_rings[Tier::TXN], local_hash_rings[Tier::TXN],
              // pushers, seed);

             // pending_requests[key].push_back(
             //  PendingTxnRequest(request_type, txn_id, payload,
             //                 response_address, response_id));
          }
        } else if (request_type == RequestType::TXN_GET || 
                   request_type == RequestType::TXN_PUT) {
          // check that this txn_id exists
          if (stored_txn_map.find(txn_id) == stored_txn_map.end()) {
              // if (!pending_requests.contains(txn_id)) {
                tp->set_error(AnnaError::TXN_DNE);
              // } else {
              //   pending_requests[key].push_back(
              //       PendingTxnRequest(request_type, key, tuple_key, payload,
              //                         response_address, response_id));
              // }
          } else {
            AnnaError error = AnnaError::NO_ERROR;
            process_put_op(txn_id, key, payload, error, serializer, stored_txn_map);
            tp->set_error(error);
            // TODO(@accheng): should txn abort if there is an error here?

            ServerThreadList key_threads = {};

            for (const Tier &tier : kStorageTiers) {
              log->info("Getting threads for storage tiers");
              key_threads = kHashRingUtil->get_responsible_threads(
                  wt.replication_response_connect_address(), request_type, 
                  txn_id, key, is_metadata(key), 
                  global_hash_rings, local_hash_rings, key_replication_map, 
                  pushers, {tier}, succeed, seed, log);
              if (key_threads.size() > 0) {
                break;
              }

              if (!succeed) { // this means we don't have the replication factor for
                              // the key
                log->info("Adding request type {} key {} to pending_requests", request_type, key);
                pending_requests[txn_id].push_back(
                  PendingTxnRequest(request_type, txn_id, key, payload,
                                    response_address, response_id));
                return;
              }
            }

            // TODO(@accheng): should just be one request?
            // send request to storage tier
            kHashRingUtil->issue_storage_request(
              wt.request_response_connect_address(), request_type, txn_id, key, 
              payload, key_threads[0], pushers); // TODO(@accheng): how should we choose thread?

            // add to pending request
            pending_requests[txn_id].push_back(
                    PendingTxnRequest(request_type, txn_id, key, payload,
                                      response_address, response_id));

            if (request_type == RequestType::TXN_PUT) {
              local_changeset.insert(key);
            }
          }
                      // send GET request to storage tier
            // look at hash ring, find right thread, kZmqUtil->send_string(
            // put in pending request
            // kHashRingUtil->issue_replication_factor_request( --> send TxnRequest to storage tier
            //     wt.replication_response_connect_address(), key, true /* txn_tier */, --> handler for pending request
            //     global_hash_rings[Tier::MEMORY], local_hash_rings[Tier::MEMORY],
            //     pushers, seed);

            // in a separate txn_reponse_handler:
            // replicate result

        } else if (request_type == RequestType::COMMIT_TXN) {
          if (stored_txn_map.find(txn_id) == stored_txn_map.end()) {
            tp->set_error(AnnaError::TXN_DNE);
          } else {
            // commit logic to storage tiers
            AnnaError error = AnnaError::NO_ERROR;
            auto ops = process_get_ops(txn_id, error, serializer, stored_txn_map);
            tp->set_error(error);
            // TODO(@accheng): should txn abort if there is an error here?
            if (error != AnnaError::NO_ERROR) {
              log->error("Unable to prepare to commit transaction");
            }

            bool abort_txn;
            vector<ServerThreadList> all_key_threads;
            for (const Operation &op: ops) {
              auto op_key = op.get_key();
              auto op_payload = op.get_value();
              ServerThreadList key_threads = {};

              for (const Tier &tier : kStorageTiers) {
                key_threads = kHashRingUtil->get_responsible_threads(
                    wt.replication_response_connect_address(), request_type, 
                    txn_id, op_key, is_metadata(op_key), 
                    global_hash_rings, local_hash_rings, key_replication_map, 
                    pushers, {tier}, succeed, seed, log);
                log->info("Getting threads for storage tiers for key {} value {} tier {} key_threads size {} suc {}", 
                  op_key, op_payload, tier, key_threads.size(), succeed);
                
                if (key_threads.size() > 0) {
                  break;
                }

                if (!succeed) { // this means we don't have the replication factor for
                                // the key
                  // TODO(@accheng): should we abort here?
                  abort_txn = true;
                  log->error("Unable to find key to prepare for commit");
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

            log->info("user_txn_request commit threads {} abort_txn {}", all_key_threads.size(), abort_txn);

            // TODO(@accheng): send abort to client
            if (abort_txn) {
              tp->set_error(AnnaError::TXN_DNE);
            } else {
              // send PREPARE_TXN requests to storage tiers
              for (unsigned i = 0; i < ops.size(); i++) {
                auto op_key = ops[i].get_key();
                auto op_payload = ops[i].get_value();
                // send prepare request to storage tier
                kHashRingUtil->issue_storage_request(
                  wt.request_response_connect_address(), RequestType::PREPARE_TXN, txn_id, // TODO(@accheng) : don't send payload
                  op_key, op_payload, all_key_threads[i][0], pushers); // TODO(@accheng): how should we choose thread?
                log->info("user_txn_request sending storage request  txn_id {} key {}", txn_id, key);

                pending_requests[txn_id].push_back(
                    PendingTxnRequest(RequestType::PREPARE_TXN, txn_id, op_key,
                                      op_payload, response_address, sresponse_id));
                log->info("Adding request type {} key {} to pending_requests", RequestType::PREPARE_TXN, key);
              }
              // this is the commit response we want to send back to the client
              // both key and op_key are txn_id
              pending_requests[txn_id].push_back(
                  PendingTxnRequest(RequestType::COMMIT_TXN, txn_id, key,
                                    "" /* payload */, response_address, response_id));
              log->info("Adding commit request type pending_requests size {}", pending_requests[txn_id].size());
            }
            // response.set_error() = error;

          }
        } else {
          log->error("Unknown request type {} in user request handler.",
                     request_type);
        }

        if (tuple.address_cache_size() > 0 &&
            tuple.address_cache_size() != threads.size()) {
          tp->set_invalidate(true);
        }

        key_access_tracker[txn_id].insert(std::chrono::system_clock::now());
        access_count += 1;
      }
    } else {
      log->info("UTR: inserting into pending_requests");
      pending_requests[txn_id].push_back(
          PendingTxnRequest(request_type, txn_id, key, payload,
                            response_address, response_id));
    }
  }

  if (response.tuples_size() > 0 && request.response_address() != "" && 
      request_type == RequestType::START_TXN) { // only return immediately for txn start
    string serialized_response;
    response.SerializeToString(&serialized_response);
    kZmqUtil->send_string(serialized_response,
                          &pushers[request.response_address()]);
  }
}