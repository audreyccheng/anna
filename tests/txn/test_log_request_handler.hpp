#include "txn/txn_handlers.hpp"

TEST_F(ServerHandlerTest, LogPrepareTest) {
  kSelfTier = Tier::LOG;
  Key key = "key";
  string value = "value";
  AnnaError error = AnnaError::NO_ERROR;

  string prepare_request = txn_prepare_key_request(kTxnId, key, ip);

  unsigned access_count = 0;
  unsigned seed = 0;

  log_request_handler(access_count, seed, prepare_request, log_, global_hash_rings,
                      local_hash_rings, pending_requests, key_access_tracker,
                      stored_key_map, key_replication_map, local_changeset, wt,
                      log_serializer, pushers);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  TxnResponse response;
  response.ParseFromString(messages[0]);

  EXPECT_EQ(response.type(), RequestType::PREPARE_TXN);
  EXPECT_EQ(response.txn_id(), kTxnId);
  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tier(), AnnaTier::ALOG);
  EXPECT_EQ(response.tuples().size(), 1);

  TxnKeyTuple rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(pending_requests.size(), 0);
  EXPECT_EQ(log_serializer->size(), 1);
}