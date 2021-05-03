#include "txn/txn_handlers.hpp"

TEST_F(ServerHandlerTest, StorageTxnGetTest) {
  Key key = "key";
  string value = "value";
  AnnaError error = AnnaError::NO_ERROR;
  base_serializer->put(kTxnId, key, value, error);
  stored_key_map[key].lock_ = 0;
  EXPECT_EQ(error, 0);
  base_serializer->commit(kTxnId, key, error);
  EXPECT_EQ(error, 0);
  unsigned num_keys = base_serializer->size();
  EXPECT_EQ(num_keys, 1);

  string get_request = txn_get_key_request(kTxnId, key, ip);

  unsigned access_count = 0;
  unsigned seed = 0;

  EXPECT_EQ(local_changeset.size(), 0);

  storage_request_handler(access_count, seed, get_request, log_, global_hash_rings,
                          local_hash_rings, pending_requests, key_access_tracker,
                          stored_key_map, key_replication_map, local_changeset, wt,
                          base_serializer, pushers);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  TxnResponse response;
  response.ParseFromString(messages[0]);

  EXPECT_EQ(response.txn_id(), kTxnId);
  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tier(), AnnaTier::AMEMORY);
  EXPECT_EQ(response.tuples().size(), 1);

  TxnKeyTuple rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.payload(), value);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 0);
  EXPECT_EQ(access_count, 1);
  EXPECT_EQ(key_access_tracker[key].size(), 1);
}

TEST_F(ServerHandlerTest, StorageTxnPutAndGetTest) {
  Key key = "key";
  string value = "value";
  string put_request = txn_put_key_request(kTxnId, key, value, ip);

  unsigned access_count = 0;
  unsigned seed = 0;

  EXPECT_EQ(local_changeset.size(), 0);

  storage_request_handler(access_count, seed, put_request, log_, global_hash_rings,
                          local_hash_rings, pending_requests, key_access_tracker,
                          stored_key_map, key_replication_map, local_changeset, wt,
                          base_serializer, pushers);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  TxnResponse response;
  response.ParseFromString(messages[0]);

  EXPECT_EQ(response.txn_id(), kTxnId);
  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tier(), AnnaTier::AMEMORY);
  EXPECT_EQ(response.tuples().size(), 1);

  TxnKeyTuple rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 1);
  EXPECT_EQ(access_count, 1);
  EXPECT_EQ(key_access_tracker[key].size(), 1);

  // Release lock
  AnnaError error = AnnaError::NO_ERROR;
  base_serializer->commit(kTxnId, key, error);
  EXPECT_EQ(error, 0);

  string get_request = txn_get_key_request(kTxnId, key, ip);

  storage_request_handler(access_count, seed, get_request, log_, global_hash_rings,
                          local_hash_rings, pending_requests, key_access_tracker,
                          stored_key_map, key_replication_map, local_changeset, wt,
                          base_serializer, pushers);

  messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 2);

  response.ParseFromString(messages[1]);

  EXPECT_EQ(response.txn_id(), kTxnId);
  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tier(), AnnaTier::AMEMORY);
  EXPECT_EQ(response.tuples().size(), 1);

  rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.payload(), value);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 1);
  EXPECT_EQ(access_count, 2);
  EXPECT_EQ(key_access_tracker[key].size(), 2);
}

TEST_F(ServerHandlerTest, StorageTxnPutAndPrepareTest) {
  Key key = "key";
  string value = "value";
  string put_request = txn_put_key_request(kTxnId, key, value, ip);

  unsigned access_count = 0;
  unsigned seed = 0;

  EXPECT_EQ(local_changeset.size(), 0);

  storage_request_handler(access_count, seed, put_request, log_, global_hash_rings,
                          local_hash_rings, pending_requests, key_access_tracker,
                          stored_key_map, key_replication_map, local_changeset, wt,
                          base_serializer, pushers);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  TxnResponse response;
  response.ParseFromString(messages[0]);

  EXPECT_EQ(response.txn_id(), kTxnId);
  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tier(), AnnaTier::AMEMORY);
  EXPECT_EQ(response.tuples().size(), 1);

  TxnKeyTuple rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 1);
  EXPECT_EQ(access_count, 1);
  EXPECT_EQ(key_access_tracker[key].size(), 1);


  string prepare_request = txn_prepare_key_request(kTxnId, key, ip);

  storage_request_handler(access_count, seed, prepare_request, log_, global_hash_rings,
                          local_hash_rings, pending_requests, key_access_tracker,
                          stored_key_map, key_replication_map, local_changeset, wt,
                          base_serializer, pushers);

  messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 2);

  // check that request to log_request_handler is well-formed
  TxnRequest request;
  request.ParseFromString(messages[1]);
  string req_rep_addr = "tcp://127.0.0.1:6260";

  EXPECT_EQ(request.txn_id(), kTxnId);
  EXPECT_EQ(request.type(), RequestType::PREPARE_TXN);
  EXPECT_EQ(request.response_address(), req_rep_addr);
  EXPECT_EQ(request.tuples().size(), 1);

  rtp = request.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(pending_requests.size(), 1);
}

TEST_F(ServerHandlerTest, StorageTxnPutAndCommitTest) {
  Key key = "key";
  string value = "value";
  string put_request = txn_put_key_request(kTxnId, key, value, ip);

  unsigned access_count = 0;
  unsigned seed = 0;

  EXPECT_EQ(local_changeset.size(), 0);

  storage_request_handler(access_count, seed, put_request, log_, global_hash_rings,
                          local_hash_rings, pending_requests, key_access_tracker,
                          stored_key_map, key_replication_map, local_changeset, wt,
                          base_serializer, pushers);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  TxnResponse response;
  response.ParseFromString(messages[0]);

  EXPECT_EQ(response.txn_id(), kTxnId);
  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tier(), AnnaTier::AMEMORY);
  EXPECT_EQ(response.tuples().size(), 1);

  TxnKeyTuple rtp = response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(local_changeset.size(), 1);
  EXPECT_EQ(access_count, 1);
  EXPECT_EQ(key_access_tracker[key].size(), 1);

  string commit_request = txn_commit_key_request(kTxnId, key, ip);

  storage_request_handler(access_count, seed, commit_request, log_, global_hash_rings,
                          local_hash_rings, pending_requests, key_access_tracker,
                          stored_key_map, key_replication_map, local_changeset, wt,
                          base_serializer, pushers);

  messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 3);

  // check that request to log_request_handler is well-formed
  TxnRequest request;
  request.ParseFromString(messages[1]);
  string req_rep_addr = "tcp://127.0.0.1:6260";

  EXPECT_EQ(request.txn_id(), kTxnId);
  EXPECT_EQ(request.type(), RequestType::COMMIT_TXN);
  EXPECT_EQ(request.response_address(), req_rep_addr);
  EXPECT_EQ(request.tuples().size(), 1);

  rtp = request.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  // check response
  response.ParseFromString(messages[2]);

  EXPECT_EQ(response.txn_id(), kTxnId);
  EXPECT_EQ(response.response_id(), kRequestId);
  EXPECT_EQ(response.tier(), AnnaTier::AMEMORY);
  EXPECT_EQ(response.tuples().size(), 1);

  rtp = request.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(pending_requests.size(), 1);

  // check lock can be held
  string new_txn_id = "0:1";
  AnnaError error = AnnaError::NO_ERROR;
  base_serializer->put(new_txn_id, key, value, error);
  EXPECT_EQ(error, 0);
}
