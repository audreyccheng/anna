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

TEST_F(ServerHandlerTest, UserTxnStartAndPutTest) {
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

  Key key = "key";
  string value = "value";
  // since replication_response_handler should be called from storage node
  // kSelfTier = Tier::MEMORY;

  string put_request = txn_put_key_request(txn_id, key, value, ip);

  user_txn_request_handler(access_count, seed, put_request, log_, global_hash_rings,
                           local_hash_rings, pending_requests, key_access_tracker,
                           stored_key_map, key_replication_map, local_changeset, wt,
                           txn_serializer, pushers);

  messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 2);

  // check that request to storage_request_handler is well-formed
  TxnRequest request;
  request.ParseFromString(messages[1]);
  string req_rep_addr = "tcp://127.0.0.1:6150";

  EXPECT_EQ(request.txn_id(), txn_id);
  EXPECT_EQ(request.type(), RequestType::TXN_PUT);
  EXPECT_EQ(request.response_address(), req_rep_addr);
  EXPECT_EQ(request.tuples().size(), 1);

  TxnKeyTuple rtp = request.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(pending_requests.size(), 1);
  EXPECT_EQ(txn_serializer->get_ops(txn_id, error).size(), 1);
  EXPECT_EQ(error, 0);
}

TEST_F(ServerHandlerTest, UserTxnStartAndGetTest) {
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

  Key key = "key";
  string value = "value";

  string get_request = txn_get_key_request(txn_id, key, ip);

  user_txn_request_handler(access_count, seed, get_request, log_, global_hash_rings,
                           local_hash_rings, pending_requests, key_access_tracker,
                           stored_key_map, key_replication_map, local_changeset, wt,
                           txn_serializer, pushers);

  messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 2);

  // check that request to storage_request_handler is well-formed
  TxnRequest request;
  request.ParseFromString(messages[1]);
  string req_rep_addr = "tcp://127.0.0.1:6150";

  EXPECT_EQ(request.txn_id(), txn_id);
  EXPECT_EQ(request.type(), RequestType::TXN_GET);
  EXPECT_EQ(request.response_address(), req_rep_addr);
  EXPECT_EQ(request.tuples().size(), 1);

  TxnKeyTuple rtp = request.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(pending_requests.size(), 1);
  EXPECT_EQ(txn_serializer->get_ops(txn_id, error).size(), 1);
  EXPECT_EQ(error, 0);
}

TEST_F(ServerHandlerTest, UserTxnStartAndCommitTest) {
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

  Key key = "key";
  string value = "value";

  txn_serializer->put_op(txn_id, key, value, error);
  EXPECT_EQ(error, 0);

  string commit_request = txn_commit_key_request(txn_id, key, ip);

  user_txn_request_handler(access_count, seed, commit_request, log_, global_hash_rings,
                           local_hash_rings, pending_requests, key_access_tracker,
                           stored_key_map, key_replication_map, local_changeset, wt,
                           txn_serializer, pushers);

  messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 2);

  // check that request to storage_request_handler is well-formed
  TxnRequest request;
  request.ParseFromString(messages[1]);
  string req_rep_addr = "tcp://127.0.0.1:6150";

  EXPECT_EQ(request.txn_id(), txn_id);
  EXPECT_EQ(request.type(), RequestType::PREPARE_TXN);
  EXPECT_EQ(request.response_address(), req_rep_addr);
  EXPECT_EQ(request.tuples().size(), 1);

  TxnKeyTuple rtp = request.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(pending_requests.size(), 1);
}
