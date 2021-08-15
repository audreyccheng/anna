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

#ifndef INCLUDE_ASYNC_TXN_CLIENT_HPP_
#define INCLUDE_ASYNC_TXN_CLIENT_HPP_

#include "anna.pb.h"
#include "common.hpp"
#include "requests.hpp"
#include "threads.hpp"
#include "types.hpp"

#include <string>

using TimePoint = std::chrono::time_point<std::chrono::system_clock>;

struct PendingTxn {
  TimePoint tp_;
  Address worker_addr_;
  TxnRequest request_;
};

inline string get_client_id_from_txn_id(const string& txn_id) {
  string::size_type n_id;

  n_id = txn_id.find("-"); // TODO(@accheng): update to constant
  string client_id = txn_id.substr(0, n_id);
  return client_id;
}

class TxnClientInterface {
 public:
  virtual void start_txn(const string& client_id) = 0;
  virtual void txn_get(const string& client_id, const string& txn_id,
                       const Key& key) = 0;
  virtual string txn_put(const string& client_id, const string& txn_id,
                         const Key& key, const string& payload) = 0;
  virtual void commit_txn(const string& client_id, const string& txn_id) = 0;
  // virtual vector<KeyResponse> receive_async() = 0;
  virtual vector<TxnResponse> receive_txn_async() = 0;
  virtual zmq::context_t* get_context() = 0;
};

class TxnClient : public TxnClientInterface {
 public:
  /**
   * @addrs A vector of routing addresses.
   * @routing_thread_count The number of thread sone ach routing node
   * @ip My node's IP address
   * @tid My client's thread ID
   * @timeout Length of request timeouts in ms
   */
  TxnClient(vector<UserRoutingThread> routing_threads, string ip,
            unsigned tid = 0, unsigned timeout = 10000) :
      routing_threads_(routing_threads),
      ut_(UserThread(ip, tid)),
      context_(zmq::context_t(1)),
      socket_cache_(SocketCache(&context_, ZMQ_PUSH)),
      key_address_puller_(zmq::socket_t(context_, ZMQ_PULL)),
      response_puller_(zmq::socket_t(context_, ZMQ_PULL)),
      log_(spdlog::basic_logger_mt("client_log_" + std::to_string(tid), "client_log_" + std::to_string(tid) + ".txt", true)),
      timeout_(timeout) {
    // initialize logger
    log_->flush_on(spdlog::level::info);

    std::hash<string> hasher;
    seed_ = time(NULL);
    seed_ += hasher(ip);
    seed_ += tid;
    // log_->info("Random seed is {}.", seed_);

    // bind the two sockets we listen on
    key_address_puller_.bind(ut_.key_address_bind_address());
    response_puller_.bind(ut_.response_bind_address());

    pollitems_ = {
        {static_cast<void*>(key_address_puller_), 0, ZMQ_POLLIN, 0},
        {static_cast<void*>(response_puller_), 0, ZMQ_POLLIN, 0},
    };

    // set the request ID to 0
    rid_ = 0;
  }

  ~TxnClient() {}

 public:
  /**
   * Issue an async START_TXN request to the KVS.
   */
  void start_txn(const string& client_id) {
    // TODO(@accheng): only issue start_txn if not in the pending map?
    if (pending_txn_response_map_.find(client_id) ==
        pending_txn_response_map_.end()) {
      TxnRequest request;
      prepare_txn_data_request(request, client_id);
      request.set_type(RequestType::START_TXN);

      try_txn_request(request, client_id);
    }
  }

  /**
   * Issue an async TXN_GET request to the KVS.
   */
  void txn_get(const string& client_id, const string& txn_id,
               const Key& key) {
    // // TODO(@accheng): Is this if needed??
    // if (pending_txn_response_map_.find(key) ==
    //     pending_txn_response_map_.end()) {
      TxnRequest request;
      prepare_txn_data_request(request, key);
      request.set_type(RequestType::TXN_GET);
      request.set_txn_id(txn_id);

      try_txn_request(request, client_id);
    // }
  }

  /**
   * Issue an async TXN_PUT request to the KVS.
   */
  string txn_put(const string& client_id, const string& txn_id,
                 const Key& key, const string& payload) {
    TxnRequest request;
    TxnKeyTuple* tuple = prepare_txn_data_request(request, key);
    request.set_type(RequestType::TXN_PUT);
    request.set_txn_id(txn_id);
    tuple->set_payload(payload);

    try_txn_request(request, client_id);
    return request.request_id();
  }

  /**
   * Issue an async COMMIT_TXN request to the KVS.
   */
  void commit_txn(const string& client_id, const string& txn_id) {
    // TODO(@accheng): only issue commit_txn if not in the pending map?
    if (pending_txn_response_map_.find(txn_id) ==
        pending_txn_response_map_.end()) {
      TxnRequest request;
      prepare_txn_data_request(request, txn_id);
      request.set_type(RequestType::COMMIT_TXN);
      request.set_txn_id(txn_id);

      try_txn_request(request, client_id);
    }
  }

  vector<TxnResponse> receive_txn_async() {
    vector<TxnResponse> result;
    kZmqUtil->poll(0, &pollitems_);

    if (pollitems_[0].revents & ZMQ_POLLIN) {
      // log_->info("Key address response");
      string serialized = kZmqUtil->recv_string(&key_address_puller_);
      KeyAddressResponse response;
      response.ParseFromString(serialized);
      Key key = response.addresses(0).key();

      if (pending_txn_map_.find(key) != pending_txn_map_.end()) {
        if (response.error() == AnnaError::NO_SERVERS) {
          // log_->error(
          //     "No servers have joined the cluster yet. Retrying request.");
          pending_txn_map_[key].first = std::chrono::system_clock::now();

          query_routing_async(key);
        } else {
          // populate cache
          for (const Address& ip : response.addresses(0).ips()) {
            key_address_cache_[key].insert(ip);
            // log_->info("Got ip {}", ip);
          }

          // handle stuff in pending request map
          for (auto& req : pending_txn_map_[key].second) {
            // if this is a START_TXN request, the txn_id doesn't yet exist
            if (req.txn_id() == "") {
              try_txn_request(req, key);
            } else {
              try_txn_request(req, get_client_id_from_txn_id(req.txn_id()));
            }
          }

          // GC the pending request map
          pending_txn_map_.erase(key);
        }
      }
    }

    if (pollitems_[1].revents & ZMQ_POLLIN) {
      // log_->info("Txn response");
      string serialized = kZmqUtil->recv_string(&response_puller_);
      TxnResponse response;
      response.ParseFromString(serialized);
      Key key;

      // log_->info("Txn response of type {} with tuple_key {}", response.type(), response.tuples(0).key());

      if (response.type() == RequestType::START_TXN) {
        key = get_client_id_from_txn_id(response.txn_id());

        // log_->info("Txn response start_txn client_id {}", key);

        if (pending_txn_response_map_.find(key) !=
            pending_txn_response_map_.end()) {
          if (key == "") {
            // error no == 2, so re-issue request
            pending_txn_response_map_[key].tp_ =
                std::chrono::system_clock::now();

            try_txn_request(pending_txn_response_map_[key].request_,
                get_client_id_from_txn_id(response.txn_id()));
          } else {
            // error no == 0 or 1
            result.push_back(response);
            pending_txn_response_map_.erase(key);
          }
        }
      } else if (response.type() == RequestType::COMMIT_TXN) {
        key = response.txn_id();

        if (pending_txn_response_map_.find(key) !=
            pending_txn_response_map_.end()) {
          if (key == "") {
            // error no == 2, so re-issue request
            pending_txn_response_map_[key].tp_ =
                std::chrono::system_clock::now();

            try_txn_request(pending_txn_response_map_[key].request_,
              get_client_id_from_txn_id(
                pending_txn_response_map_[key].request_.txn_id()));
          } else {
            // error no == 0 or 1
            result.push_back(response);
            pending_txn_response_map_.erase(key);
          }
        }
      } else {
        key = response.tuples(0).key();

        if (pending_txn_key_response_map_.find(key) !=
                pending_txn_key_response_map_.end() &&
            pending_txn_key_response_map_[key].find(response.response_id()) !=
                pending_txn_key_response_map_[key].end()) {
          if (check_txn_tuple(response.tuples(0))) {
            // error no == 2, so re-issue request
            pending_txn_key_response_map_[key][response.response_id()].tp_ =
                std::chrono::system_clock::now();

            try_txn_request(pending_txn_key_response_map_[key][response.response_id()]
                            .request_, get_client_id_from_txn_id(response.txn_id()));
          } else {
            // error no == 0
            result.push_back(response);
            pending_txn_key_response_map_[key].erase(response.response_id());

            if (pending_txn_key_response_map_[key].size() == 0) {
              pending_txn_key_response_map_.erase(key);
            }
          }
        }
      }
    }

    // GC the pending txn request map
    set<Key> to_remove;
    for (const auto& pair : pending_txn_map_) {
      if (std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now() - pair.second.first)
              .count() > timeout_) {
        // query to the routing tier timed out
        for (const auto& req : pair.second.second) {
          result.push_back(generate_bad_txn_response(req));
        }

        to_remove.insert(pair.first);
      }
    }

    for (const Key& key : to_remove) {
      pending_txn_map_.erase(key);
    }

    // GC the pending txn response map
    to_remove.clear();
    for (const auto& pair : pending_txn_response_map_) {
      if (std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now() - pair.second.tp_)
              .count() > timeout_) {
        // query to server timed out
        result.push_back(generate_bad_txn_response(pair.second.request_));
        to_remove.insert(pair.first);
        invalidate_cache_for_worker(pair.second.worker_addr_);
      }
    }

    for (const Key& key : to_remove) {
      pending_txn_response_map_.erase(key);
    }

    // GC the pending txn put and get response map
    map<Key, set<string>> to_remove_put;
    for (const auto& key_map_pair : pending_txn_key_response_map_) {
      for (const auto& id_map_pair :
           pending_txn_key_response_map_[key_map_pair.first]) {
        if (std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now() -
                pending_txn_key_response_map_[key_map_pair.first][id_map_pair.first]
                    .tp_)
                .count() > timeout_) {
          result.push_back(generate_bad_txn_response(id_map_pair.second.request_));
          to_remove_put[key_map_pair.first].insert(id_map_pair.first);
          invalidate_cache_for_worker(id_map_pair.second.worker_addr_);
        }
      }
    }

    for (const auto& key_set_pair : to_remove_put) {
      for (const auto& id : key_set_pair.second) {
        pending_txn_key_response_map_[key_set_pair.first].erase(id);
      }
    }

    return result;
  }

  /**
   * Set the logger used by the client.
   */
  void set_logger(logger log) { log_ = log; }

  /**
   * Clears the key address cache held by this client.
   */
  void clear_cache() { key_address_cache_.clear(); }

  /**
   * Return the ZMQ context used by this client.
   */
  zmq::context_t* get_context() { return &context_; }

  /**
   * Return the random seed used by this client.
   */
  unsigned get_seed() { return seed_; }

 private:
  /**
   * A recursive helper method for that tries
   * to issue a request at most trial_limit times before giving up. It  checks
   * for the default failure modes (timeout, errno == 2, and cache
   * invalidation). If there are no issues, it returns the set of responses to
   * the respective implementations for them to deal with.
   */
  void try_txn_request(TxnRequest& request, const string &client_id) {
    // we only get NULL back for the worker thread if the query to the routing
    // tier timed out, which should never happen.
    Key key = request.tuples(0).key();
    // log_->info("Trying request with type {} and key {}", request.type(), key);

    Address worker = get_worker_thread(client_id);
    if (worker.length() == 0) {
      // log_->info("No worker threads for client {} yet", client_id);
      // this means a key addr request is issued asynchronously
      if (pending_txn_map_.find(key) == pending_txn_map_.end()) {
        pending_txn_map_[key].first = std::chrono::system_clock::now();
      }
      pending_txn_map_[key].second.push_back(request);
      return;
    }

    request.mutable_tuples(0)->set_address_cache_size(
        key_address_cache_[key].size());

    send_request<TxnRequest>(request, socket_cache_[worker]);

    // Use client id as key in map of pending responses
    // TODO(@accheng) Ok to assume each client only starts 1 txn at a time?
    if (request.type() == RequestType::START_TXN || 
        request.type() == RequestType::COMMIT_TXN) {
      if (pending_txn_response_map_.find(key) ==
          pending_txn_response_map_.end()) {
        pending_txn_response_map_[key].tp_ = std::chrono::system_clock::now();
        pending_txn_response_map_[key].request_ = request;
      }

      pending_txn_response_map_[key].worker_addr_ = worker;
    } else {
      if (pending_txn_key_response_map_[key].find(request.request_id()) ==
          pending_txn_key_response_map_[key].end()) {
        pending_txn_key_response_map_[key][request.request_id()].tp_ =
            std::chrono::system_clock::now();
        pending_txn_key_response_map_[key][request.request_id()].request_ = request;
      }
      pending_txn_key_response_map_[key][request.request_id()].worker_addr_ =
          worker;
    }
  }

  /**
   * A helper method to check for the default failure modes for a request that
   * retrieves a response. It returns true if the caller method should reissue
   * the request (this happens if errno == 2). Otherwise, it returns false. It
   * invalidates the local cache if the information is out of date.
   */
  bool check_txn_tuple(const TxnKeyTuple& tuple) {
    Key key = tuple.key();
    if (tuple.error() == 2) {
      // log_->info(
      //     "Server ordered invalidation of key address cache for key {}. "
      //     "Retrying request.",
      //     key);

      invalidate_txn_cache_for_key(key, tuple);
      return true;
    }

    if (tuple.invalidate()) {
      invalidate_txn_cache_for_key(key, tuple);

      // log_->info("Server ordered invalidation of key address cache for key {}",
      //            key);
    }

    return false;
  }

  /**
   * When a server thread tells us to invalidate the cache for a key it's
   * because we likely have out of date information for that key; it sends us
   * the updated information for that key, and update our cache with that
   * information.
   */
  void invalidate_txn_cache_for_key(const Key& key, const TxnKeyTuple& tuple) {
    key_address_cache_.erase(key);
  }

  /**
   * Invalidate the key caches for any key that previously had this worker in
   * its cache. The underlying assumption is that if the worker timed out, it
   * might have failed, and so we don't want to rely on it being alive for both
   * the key we were querying and any other key.
   */
  void invalidate_cache_for_worker(const Address& worker) {
    vector<string> tokens;
    split(worker, ':', tokens);
    string signature = tokens[1];
    set<Key> remove_set;

    for (const auto& key_pair : key_address_cache_) {
      for (const string& address : key_pair.second) {
        vector<string> v;
        split(address, ':', v);

        if (v[1] == signature) {
          remove_set.insert(key_pair.first);
        }
      }
    }

    for (const string& key : remove_set) {
      key_address_cache_.erase(key);
    }
  }

  /**
   * Prepare a data request object by populating the request ID, the key for
   * the request, and the response address. This method modifies the passed-in
   * KeyRequest and also returns a pointer to the KeyTuple contained by this
   * request.
   */
  TxnKeyTuple* prepare_txn_data_request(TxnRequest& request, const Key& key) {
    request.set_request_id(get_request_id());
    request.set_response_address(ut_.response_connect_address());
    
    TxnKeyTuple* tp = request.add_tuples();
    tp->set_key(key);

    return tp;
  }

  /**
   * returns all the worker threads for the key queried. If there are no cached
   * threads, a request is sent to the routing tier. If the query times out,
   * NULL is returned.
   */
  set<Address> get_all_worker_threads(const Key& key) {
    if (key_address_cache_.find(key) == key_address_cache_.end() ||
        key_address_cache_[key].size() == 0) {
      if (pending_txn_map_.find(key) == pending_txn_map_.end()) {
        query_routing_async(key);
      }
      return set<Address>();
    } else {
      return key_address_cache_[key];
    }
  }

  /**
   * Similar to the previous method, but only returns one (randomly chosen)
   * worker address instead of all of them.
   */
  Address get_worker_thread(const Key& key) {
    set<Address> local_cache = get_all_worker_threads(key);

    // This will be empty if the worker threads are not cached locally
    if (local_cache.size() == 0) {
      return "";
    }

    return *(next(begin(local_cache), rand_r(&seed_) % local_cache.size()));
  }

  /**
   * Returns one random routing thread's key address connection address. If the
   * client is running outside of the cluster (ie, it is querying the ELB),
   * there's only one address to choose from but 4 threads.
   */
  Address get_routing_thread() {
    return routing_threads_[rand_r(&seed_) % routing_threads_.size()]
        .key_address_connect_address();
  }

  /**
   * Send a query to the routing tier asynchronously.
   */
  void query_routing_async(const Key& key) {
    // define protobuf request objects
    KeyAddressRequest request;

    // populate request with response address, request id, etc.
    request.set_request_id(get_request_id());
    request.set_response_address(ut_.key_address_connect_address());
    request.add_keys(key);
    request.set_tier(AnnaTier::ATXN);

    Address rt_thread = get_routing_thread();
    send_request<KeyAddressRequest>(request, socket_cache_[rt_thread]); // --> this should only return txn threads
  }

  /**
   * Generates a unique request ID.
   */
  string get_request_id() {
    if (++rid_ % 10000 == 0) rid_ = 0;
    return ut_.ip() + ":" + std::to_string(ut_.tid()) + "_" +
           std::to_string(rid_++);
  }

  TxnResponse generate_bad_txn_response(const TxnRequest& req) {
    TxnResponse resp;

    resp.set_type(req.type());
    resp.set_response_id(req.request_id());
    resp.set_txn_id(req.txn_id());
    resp.set_error(AnnaError::TIMEOUT);
    return resp;
  }

 private:
  // the set of routing addresses outside the cluster
  vector<UserRoutingThread> routing_threads_;

  // the current request id
  unsigned rid_;

  // the random seed for this client
  unsigned seed_;

  // the IP and port functions for this thread
  UserThread ut_;

  // the ZMQ context we use to create sockets
  zmq::context_t context_;

  // cache for opened sockets
  SocketCache socket_cache_;

  // ZMQ receiving sockets
  zmq::socket_t key_address_puller_;
  zmq::socket_t response_puller_;

  vector<zmq::pollitem_t> pollitems_;

  // cache for retrieved worker addresses organized by key
  map<Key, set<Address>> key_address_cache_;

  // class logger
  logger log_;

  // GC timeout
  unsigned timeout_;

  // keeps track of pending txn requests due to missing worker address
  map<Key, pair<TimePoint, vector<TxnRequest>>> pending_txn_map_;

  // keeps track of pending txn start and commit responses
  map<string, PendingTxn> pending_txn_response_map_;

  // keeps track of pending txn put and get responses
  // map<txn_id, <request_id, PendingTxnRequest>> 
  map<string, map<string, PendingTxn>> pending_txn_key_response_map_;
};

#endif  // INCLUDE_ASYNC_TXN_CLIENT_HPP_
