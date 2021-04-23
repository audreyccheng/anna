#include "txn/txn_handlers.hpp"

TEST_F(ServerHandlerTest, UserTxnStartTest) {
  kSelfTier = Tier::TXN;
  string client_id = "0";
  AnnaError error = AnnaError::NO_ERROR;

  // Assume this client has already sent requests before
  stored_key_map[client_id].num_ops_ = 0;

  string start_request = txn_start_request(client_id, ip);

  unsigned access_count = 0;
  unsigned seed = 0;

  user_txn_request_handler(access_count, seed, start_request, log_, global_hash_rings,
                           local_hash_rings, pending_requests, key_access_tracker,
                           stored_key_map, key_replication_map, local_changeset, wt,
                           txn_serializer, pushers);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  TxnResponse response;
  response.ParseFromString(messages[0]);
  string txn_id = response.txn_id();

  EXPECT_NE(txn_id, "");
  EXPECT_NE(stored_key_map.find(txn_id), stored_key_map.end());
  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tier(), AnnaTier::ATXN);
  EXPECT_EQ(response.tuples().size(), 1);

}