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

TEST_F(RoutingHandlerTest, Address) {
  EXPECT_EQ(global_hash_rings[Tier::MEMORY].size(), 3000);

  unsigned seed = 0;

  // Test storage tier
  KeyAddressRequest req;
  req.set_request_id("1");
  req.set_response_address("tcp://127.0.0.1:5000");
  req.set_txn_tier(false);
  req.add_keys("key");

  string serialized;
  req.SerializeToString(&serialized);

  address_handler(log_, serialized, pushers, rt, global_hash_rings,
                  local_hash_rings, key_replication_map, pending_requests,
                  seed);

  vector<string> messages = get_zmq_messages();

  EXPECT_EQ(messages.size(), 1);
  string serialized_resp = messages[0];

  KeyAddressResponse resp;
  resp.ParseFromString(serialized_resp);

  EXPECT_EQ(resp.response_id(), "1");
  EXPECT_EQ(resp.error(), 0);

  for (const KeyAddressResponse_KeyAddress &addr : resp.addresses()) {
    string key = addr.key();
    EXPECT_EQ(key, "key");
    for (const string &ip : addr.ips()) {
      EXPECT_EQ(ip, "tcp://127.0.0.1:6230");
    }
  }

  // Test transactional tier
  KeyAddressRequest txn_req;
  txn_req.set_request_id("2");
  txn_req.set_response_address("tcp://127.0.0.1:5000");
  txn_req.set_txn_tier(false);
  txn_req.add_keys("txn_key");

  string txn_serialized;
  txn_req.SerializeToString(&txn_serialized);

  address_handler(log_, txn_serialized, pushers, rt, global_hash_rings,
                  local_hash_rings, key_replication_map, pending_requests,
                  seed);

  vector<string> txn_messages = get_zmq_messages();

  EXPECT_EQ(txn_messages.size(), 1);
  string txn_serialized_resp = txn_messages[1];

  KeyAddressResponse txn_resp;
  txn_resp.ParseFromString(txn_serialized_resp);

  EXPECT_EQ(txn_resp.response_id(), "2");
  EXPECT_EQ(txn_resp.error(), 0);

  for (const KeyAddressResponse_KeyAddress &addr : txn_resp.addresses()) {
    string key = addr.key();
    EXPECT_EQ(key, "txn_key");
    for (const string &ip : addr.ips()) {
      EXPECT_EQ(ip, "tcp://127.0.0.1:6220");
    }
  }
}
