#include "txn/txn_handlers.hpp"

void user_txn_request_handler(
    unsigned &access_count, unsigned &seed, string &serialized, logger log,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    // map<Key, vector<PendingRequest>> &pending_requests,
    map<string, vector<PendingTxnRequest>> &pending_requests, // <txn_id, <request_id, PendingTxnRequest>> 
    map<Key, std::multiset<TimePoint>> &key_access_tracker,
    map<Key, TxnKeyProperty> &stored_txn_map,
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, TxnSerializer &serializer, SocketCache &pushers) {
  TxnRequest request;
  request.ParseFromString(serialized);

  TxnResponse response;
  string response_id = request.request_id();
  response.set_response_id(request.request_id());

  response.set_type(request.type());
  response.set_tier(kSelfTier);

  bool succeed;
  RequestType request_type = request.type();
  string response_address = request.response_address();
  string txn_id = request.txn_id();

  // TODO(@accheng): should only be one tuple?
  for (const auto &tuple : request.tuples()) {
    // first check if the thread is responsible for the key
    Key tuple_key = tuple.key();
    string payload = tuple.payload();

    Key key = txn_id;
    if (request_type == RequestType::START_TXN) {
      // a START_TXN request doesn't have a txn_id so we use client_id
      key = tuple_key;
    }

    ServerThreadList threads = kHashRingUtil->get_responsible_threads(
        wt.replication_response_connect_address(), key, is_metadata(key), 
        global_hash_rings, local_hash_rings, key_replication_map, 
        pushers, kSelfTierIdVector, succeed, seed);

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
              wt.replication_response_connect_address(), key, Tier::TXN, 
              global_hash_rings[Tier::TXN], local_hash_rings[Tier::TXN],
              pushers, seed);

          // since this is a new client request, key is client_id instead of txn_id
          pending_requests[key].push_back( 
              PendingTxnRequest(request_type, key, payload,
                             response_address, response_id));
        }
      } else { // if we know the responsible threads, we process the request
        TxnKeyTuple *tp = response.add_tuples();
        tp->set_key(key);

        if (request_type == RequestType::START_TXN) {
          // if this is a replication request, signal that this key doesn't yet exist
          if (stored_txn_map.find(key) == stored_txn_map.end() && is_metadata(key)) {
            tp->set_error(AnnaError::TXN_DNE);
          } else { // TODO(@accheng): update
            auto txn_id = process_start_txn(key, serializer, stored_txn_map); // add txn_id to stored_key_map
            response.set_txn_id(txn_id);

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
        } else if (request_type == RequestType::TXN_GET) {
          // check if the START_TXN for this request is still pending
          // otherwise, this txn does not exist
          if (stored_txn_map.find(key) == stored_txn_map.end())
              if (!pending_requests.contains(txn_id)) {
                tp->set_error(AnnaError::TXN_DNE);
              } else {
                pending_requests[key].push_back(
                    PendingTxnRequest(request_type, key, tuple_key, payload,
                                      response_address, response_id));
              }
          } else {
            AnnaError error = AnnaError::NO_ERROR;
            process_put_op(txn_id, key, payload, error, serializer, stored_txn_map);
            tp->set_error(error);

            ServerThreadList key_threads = {};

            for (const Tier &tier : kStorageTiers) {
              key_threads = kHashRingUtil->get_responsible_threads(
                  wt.replication_response_connect_address(), tuple_key, is_metadata(tuple_key), 
                  global_hash_rings, local_hash_rings, key_replication_map, 
                  pushers, {tier}, succeed, seed);
              if (threads.size() > 0) {
                break;
              }

              if (!succeed) { // this means we don't have the replication factor for
                              // the key
                // pending_requests[key].push_back(std::pair<Address, string>(
                //     addr_request.response_address(), addr_request.request_id()));
                // return;
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

            for (const ServerThread &thread : threads) {
              // send GET request to storage tier
            }

            // add to pending request regardless
            pending_requests[key].push_back(
                    PendingTxnRequest(request_type, key, tuple_key, payload,
                                      response_address, response_id));

            local_changeset.insert(key);
          }
        } else if (request_type == RequestType::COMMIT_TXN) {
          if (stored_txn_map.find(request.txn_id()) == stored_txn_map.end()) {
            tp->set_error(AnnaError::TXN_DNE);
          } else {
            // commit logic to storage tiers
            response.set_error() = error;


            AnnaError error = AnnaError::NO_ERROR;
            process_commit_txn(txn_id, error, serializer);
            // tp->set_error(error);
            if (error != AnnaError::NO_ERROR) {
              log->error("Unable to find transaction to commit");
            }
            response.set_error() = error;
          }
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
          PendingTxnRequest(request_type, key, tuple_key, payload,
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