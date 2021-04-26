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

#include "mock/mock_hash_utils.hpp"
#include "mock_zmq_utils.hpp"

MockZmqUtil mock_zmq_util;
ZmqUtilInterface *kZmqUtil = &mock_zmq_util;

MockHashRingUtil mock_hash_ring_util;
HashRingUtilInterface *kHashRingUtil = &mock_hash_ring_util;

logger log_ = spdlog::basic_logger_mt("mock_log", "mock_log.txt", true);

string kRequestId = "0";
string kTxnId = "0:0";

class ServerHandlerTest : public ::testing::Test {
protected:
  Address ip = "127.0.0.1";
  unsigned thread_id = 0;
  GlobalRingMap global_hash_rings;
  LocalRingMap local_hash_rings;
  map<Key, TxnKeyProperty> stored_key_map;
  map<Key, KeyReplication> key_replication_map;
  ServerThread wt;
  map<Key, vector<PendingTxnRequest>> pending_requests;
  // map<Key, vector<PendingGossip>> pending_gossip;
  map<Key, std::multiset<TimePoint>> key_access_tracker;
  set<Key> local_changeset;

  zmq::context_t context;
  SocketCache pushers = SocketCache(&context, ZMQ_PUSH);
  // SerializerMap serializers;

  TxnSerializer *txn_serializer;
  BaseSerializer *base_serializer;
  LogSerializer *log_serializer;
  BaseTxn *base_txn_node;
  LockStore *base_node;
  BaseLog *base_log_node;

  ServerHandlerTest() {
    base_txn_node = new BaseTxn();
    txn_serializer = new BaseTxnSerializer(base_txn_node);

    base_node = new LockStore();
    base_serializer = new LockStoreSerializer(base_node);

    base_log_node = new BaseLog();
    log_serializer = new BaseLogSerializer(base_log_node);

    wt = ServerThread(ip, ip, thread_id);
    global_hash_rings[Tier::MEMORY].insert(ip, ip, 0, thread_id);
  }

  virtual ~ServerHandlerTest() {
    delete base_txn_node;
    delete base_node;
    delete base_log_node;
    delete txn_serializer;
    delete base_serializer;
    delete log_serializer;
  }

public:
  void SetUp() {
    // reset all global variables
    kDefaultLocalReplication = 1;
    kDefaultGlobalTxnReplication = 1;
    kDefaultGlobalMemoryReplication = 1;
    kDefaultGlobalEbsReplication = 1;
    
    kDefaultLocalReplication = 1;
    kSelfTier = Tier::MEMORY;
    kThreadNum = 1;
    kSelfTierIdVector = {kSelfTier};
  }

  void TearDown() {
    // clear all the logged messages after each test
    mock_zmq_util.sent_messages.clear();
  }

  vector<string> get_zmq_messages() { return mock_zmq_util.sent_messages; }

  void warmup_key_replication_map_to_defaults(vector<string> keys) {
    for (string key : keys) {
      key_replication_map[key].global_replication_[Tier::TXN] =
          kDefaultGlobalTxnReplication;
      key_replication_map[key].global_replication_[Tier::MEMORY] =
          kDefaultGlobalMemoryReplication;
      key_replication_map[key].global_replication_[Tier::DISK] =
          kDefaultGlobalEbsReplication;
      // key_replication_map[key].global_replication_[Tier::LOG] =
      //     kDefaultGlobalLogReplication;
      key_replication_map[key].local_replication_[Tier::TXN] =
          kDefaultLocalReplication;
      key_replication_map[key].local_replication_[Tier::MEMORY] =
          kDefaultLocalReplication;
      key_replication_map[key].local_replication_[Tier::DISK] =
          kDefaultLocalReplication;
      // key_replication_map[key].local_replication_[Tier::LOG] =
      //     kDefaultLocalReplication;
    }
  }

  string txn_start_request(string client_id, string ip) {
    TxnRequest request;
    request.set_type(RequestType::START_TXN);
    request.set_response_address(UserThread(ip, 0).response_connect_address());
    request.set_request_id(kRequestId);

    TxnKeyTuple *tp = request.add_tuples();
    tp->set_key(std::move(client_id));

    string request_str;
    request.SerializeToString(&request_str);

    return request_str;
  }

  // NOTE: Pass in an empty string to avoid putting something into the
  // serializer
  string txn_get_key_request(string txn_id, Key key, string ip) {
    TxnRequest request;
    request.set_type(RequestType::TXN_GET);
    request.set_response_address(UserThread(ip, 0).response_connect_address());
    request.set_request_id(kRequestId);
    request.set_txn_id(txn_id);

    TxnKeyTuple *tp = request.add_tuples();
    tp->set_key(std::move(key));

    string request_str;
    request.SerializeToString(&request_str);

    return request_str;
  }

  string txn_put_key_request(string txn_id, Key key, string payload, string ip) {
    TxnRequest request;
    request.set_type(RequestType::TXN_PUT);
    request.set_response_address(UserThread(ip, 0).response_connect_address());
    request.set_request_id(kRequestId);
    request.set_txn_id(txn_id);

    TxnKeyTuple *tp = request.add_tuples();
    tp->set_key(std::move(key));
    tp->set_payload(std::move(payload));

    string request_str;
    request.SerializeToString(&request_str);

    return request_str;
  }

  string txn_prepare_key_request(string txn_id, Key key, string ip) {
    TxnRequest request;
    request.set_type(RequestType::PREPARE_TXN);
    request.set_response_address(UserThread(ip, 0).response_connect_address());
    request.set_request_id(kRequestId);
    request.set_txn_id(txn_id);

    TxnKeyTuple *tp = request.add_tuples();
    tp->set_key(std::move(key));

    string request_str;
    request.SerializeToString(&request_str);

    return request_str;
  }

  string txn_commit_key_request(string txn_id, Key key, string ip) {
    TxnRequest request;
    request.set_type(RequestType::COMMIT_TXN);
    request.set_response_address(UserThread(ip, 0).response_connect_address());
    request.set_request_id(kRequestId);
    request.set_txn_id(txn_id);

    TxnKeyTuple *tp = request.add_tuples();
    tp->set_key(std::move(key));

    string request_str;
    request.SerializeToString(&request_str);

    return request_str;
  }
};
