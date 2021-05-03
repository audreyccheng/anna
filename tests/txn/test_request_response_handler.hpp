#include "txn/txn_handlers.hpp"

//////////////////////////////// TXN TESTS ////////////////////////////////

TEST_F(ServerHandlerTest, TxnPutRequestResponse) {
  kSelfTier = Tier::TXN;
  string txn_id = "0:0";
  Key key = "key";
  string value = "value";
  AnnaError error = AnnaError::NO_ERROR;

  // Assume this txn exists
  stored_key_map[txn_id].num_ops_ = 0;
  pending_requests[txn_id].push_back(
  	PendingTxnRequest(RequestType::TXN_PUT, txn_id, key, value,
                      UserThread(ip, 0).response_connect_address(), kRequestId));

  TxnResponse response;
  response.set_type(RequestType::TXN_PUT);
  response.set_response_id(kRequestId);
  response.set_tier(AnnaTier::AMEMORY);
  response.set_txn_id(txn_id);

  TxnKeyTuple *tp = response.add_tuples();
  tp->set_key(key);
  tp->set_error(AnnaError::NO_ERROR);
  tp->set_payload(value);

  string put_response;
  response.SerializeToString(&put_response);

  unsigned access_count = 0;
  unsigned seed = 0;

  request_response_handler(seed, access_count, log_, put_response, global_hash_rings,
                           local_hash_rings, pending_requests, // pending_gossip,
                           key_access_tracker, stored_key_map, key_replication_map,
                           local_changeset, wt, txn_serializer, base_serializer,
                           log_serializer, pushers);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  TxnResponse rep_response;
  rep_response.ParseFromString(messages[0]);

  EXPECT_EQ(rep_response.txn_id(), txn_id);
  EXPECT_EQ(rep_response.response_id(), kRequestId);
  EXPECT_EQ(rep_response.tier(), AnnaTier::ATXN);
  EXPECT_EQ(rep_response.tuples().size(), 1);

  TxnKeyTuple rtp = rep_response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.payload(), value);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(pending_requests.size(), 0);
}

TEST_F(ServerHandlerTest, TxnPrepareRequestResponse) {
  kSelfTier = Tier::TXN;
  string txn_id = txn_serializer->create_txn("0");
  Key key = "key";
  Key next_key = "key1";
  string value = "value";
  AnnaError error = AnnaError::NO_ERROR;

  // Assume this txn exists
  txn_serializer->put_op(txn_id, key, value, error);
  txn_serializer->put_op(txn_id, next_key, value, error);
  auto ops = process_get_ops(txn_id, error, txn_serializer, stored_key_map);
  EXPECT_EQ(ops.size(), 2);
  stored_key_map[txn_id].num_ops_ = 0;
  pending_requests[txn_id].push_back(
    PendingTxnRequest(RequestType::COMMIT_TXN, txn_id, "", "",
                      UserThread(ip, 0).response_connect_address(), kRequestId));
  pending_requests[txn_id].push_back(
    PendingTxnRequest(RequestType::PREPARE_TXN, txn_id, key, value,
                      UserThread(ip, 0).response_connect_address(), kRequestId));
  pending_requests[txn_id].push_back(
    PendingTxnRequest(RequestType::PREPARE_TXN, txn_id, next_key, value,
                      UserThread(ip, 0).response_connect_address(), kRequestId));
  EXPECT_EQ(pending_requests[txn_id].size(), 3);

  TxnResponse response;
  response.set_type(RequestType::PREPARE_TXN);
  response.set_response_id(kRequestId);
  response.set_tier(AnnaTier::AMEMORY);
  response.set_txn_id(txn_id);

  TxnKeyTuple *tp = response.add_tuples();
  tp->set_key(key);
  tp->set_error(AnnaError::NO_ERROR);

  string prepare_response;
  response.SerializeToString(&prepare_response);

  unsigned access_count = 0;
  unsigned seed = 0;

  request_response_handler(seed, access_count, log_, prepare_response, global_hash_rings,
                           local_hash_rings, pending_requests, // pending_gossip,
                           key_access_tracker, stored_key_map, key_replication_map,
                           local_changeset, wt, txn_serializer, base_serializer,
                           log_serializer, pushers);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 0);

  EXPECT_EQ(pending_requests[txn_id].size(), 2);

  TxnResponse new_response;
  new_response.set_type(RequestType::PREPARE_TXN);
  new_response.set_response_id(txn_id);
  new_response.set_tier(AnnaTier::AMEMORY);
  new_response.set_txn_id(txn_id);

  TxnKeyTuple *new_tp = new_response.add_tuples();
  new_tp->set_key(next_key);
  new_tp->set_error(AnnaError::NO_ERROR);

  string new_prepare_response;
  new_response.SerializeToString(&new_prepare_response);

  request_response_handler(seed, access_count, log_, new_prepare_response, global_hash_rings,
                           local_hash_rings, pending_requests, // pending_gossip,
                           key_access_tracker, stored_key_map, key_replication_map,
                           local_changeset, wt, txn_serializer, base_serializer,
                           log_serializer, pushers);

  messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 3);

  // check requests to storage tier are well-formed
  TxnRequest request;
  request.ParseFromString(messages[0]);

  string req_rep_addr = "tcp://127.0.0.1:6260";

  EXPECT_EQ(request.txn_id(), txn_id);
  EXPECT_EQ(request.type(), RequestType::COMMIT_TXN);
  EXPECT_EQ(request.response_address(), req_rep_addr);
  EXPECT_EQ(request.tuples().size(), 1);

  TxnKeyTuple rtp = request.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  TxnRequest next_request;
  next_request.ParseFromString(messages[1]);

  EXPECT_EQ(next_request.txn_id(), txn_id);
  EXPECT_EQ(next_request.type(), RequestType::COMMIT_TXN);
  EXPECT_EQ(next_request.response_address(), req_rep_addr);
  EXPECT_EQ(next_request.tuples().size(), 1);

  rtp = next_request.tuples(0);

  EXPECT_EQ(rtp.key(), next_key);
  EXPECT_EQ(rtp.error(), 0);

  // check response to client is well-formed
  TxnResponse commit_response;
  commit_response.ParseFromString(messages[2]);

  EXPECT_EQ(commit_response.txn_id(), txn_id);
  EXPECT_EQ(commit_response.response_id(), kRequestId);
  EXPECT_EQ(commit_response.type(), RequestType::COMMIT_TXN);
  EXPECT_EQ(commit_response.tier(), AnnaTier::ATXN);
  EXPECT_EQ(commit_response.tuples().size(), 1);

  rtp = commit_response.tuples(0);

  EXPECT_EQ(rtp.key(), next_key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(txn_serializer->size(txn_id), 2);
  EXPECT_EQ(pending_requests[txn_id].size(), 2);
}

TEST_F(ServerHandlerTest, TxnCommitRequestResponse) {
  kSelfTier = Tier::TXN;
  string txn_id = txn_serializer->create_txn("0");
  Key key = "key";
  Key next_key = "key1";
  string value = "value";
  AnnaError error = AnnaError::NO_ERROR;

  // Assume this txn exists
  txn_serializer->put_op(txn_id, key, value, error);
  stored_key_map[txn_id].num_ops_ = 0;
  pending_requests[txn_id].push_back(
  	PendingTxnRequest(RequestType::COMMIT_TXN, txn_id, key, value,
                      UserThread(ip, 0).response_connect_address(), kRequestId));
  pending_requests[txn_id].push_back(
  	PendingTxnRequest(RequestType::COMMIT_TXN, txn_id, next_key, value,
                      UserThread(ip, 0).response_connect_address(), kRequestId));

  TxnResponse response;
  response.set_type(RequestType::COMMIT_TXN);
  response.set_response_id(kRequestId);
  response.set_tier(AnnaTier::AMEMORY);
  response.set_txn_id(txn_id);

  TxnKeyTuple *tp = response.add_tuples();
  tp->set_key(key);
  tp->set_error(AnnaError::NO_ERROR);

  string commit_response;
  response.SerializeToString(&commit_response);

  unsigned access_count = 0;
  unsigned seed = 0;

  request_response_handler(seed, access_count, log_, commit_response, global_hash_rings,
                           local_hash_rings, pending_requests, // pending_gossip,
                           key_access_tracker, stored_key_map, key_replication_map,
                           local_changeset, wt, txn_serializer, base_serializer,
                           log_serializer, pushers);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 0);

  EXPECT_EQ(pending_requests[txn_id].size(), 1);

  TxnResponse new_response;
  new_response.set_type(RequestType::COMMIT_TXN);
  new_response.set_response_id(kRequestId);
  new_response.set_tier(AnnaTier::AMEMORY);
  new_response.set_txn_id(txn_id);

  TxnKeyTuple *new_tp = new_response.add_tuples();
  new_tp->set_key(next_key);
  new_tp->set_error(AnnaError::NO_ERROR);

  string new_commit_response;
  new_response.SerializeToString(&new_commit_response);

  request_response_handler(seed, access_count, log_, new_commit_response, global_hash_rings,
                           local_hash_rings, pending_requests, // pending_gossip,
                           key_access_tracker, stored_key_map, key_replication_map,
                           local_changeset, wt, txn_serializer, base_serializer,
                           log_serializer, pushers);

  messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 0);

  EXPECT_EQ(txn_serializer->size(txn_id), 0);
  EXPECT_EQ(stored_key_map.size(), 0);
  EXPECT_EQ(pending_requests.size(), 0);
  
  // TxnResponse rep_response;
  // rep_response.ParseFromString(messages[0]);

  // EXPECT_EQ(rep_response.txn_id(), txn_id);
  // EXPECT_EQ(rep_response.response_id(), kRequestId);
  // EXPECT_EQ(rep_response.tier(), AnnaTier::ATXN);
  // EXPECT_EQ(rep_response.tuples().size(), 1);

  // TxnKeyTuple rtp = rep_response.tuples(0);

  // EXPECT_EQ(rtp.key(), next_key);
  // EXPECT_EQ(rtp.error(), 0);
}

//////////////////////////////// STORAGE TESTS ////////////////////////////////

TEST_F(ServerHandlerTest, StoragePrepareRequestResponse) {
  kSelfTier = Tier::MEMORY;
  Key key = "key";
  string value = "value";
  AnnaError error = AnnaError::NO_ERROR;

  pending_requests[key].push_back( // storage pending_requests indexed on key
    PendingTxnRequest(RequestType::PREPARE_TXN, kTxnId, key, value,
                      UserThread(ip, 0).response_connect_address(), kRequestId));

  TxnResponse response;
  response.set_type(RequestType::PREPARE_TXN);
  response.set_response_id(kRequestId);
  response.set_tier(AnnaTier::ALOG);
  response.set_txn_id(kTxnId);

  TxnKeyTuple *tp = response.add_tuples();
  tp->set_key(key);
  tp->set_error(AnnaError::NO_ERROR);

  string put_response;
  response.SerializeToString(&put_response);

  unsigned access_count = 0;
  unsigned seed = 0;

  request_response_handler(seed, access_count, log_, put_response, global_hash_rings,
                           local_hash_rings, pending_requests, // pending_gossip,
                           key_access_tracker, stored_key_map, key_replication_map,
                           local_changeset, wt, txn_serializer, base_serializer,
                           log_serializer, pushers);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  TxnResponse rep_response;
  rep_response.ParseFromString(messages[0]);

  EXPECT_EQ(rep_response.txn_id(), kTxnId);
  EXPECT_EQ(rep_response.response_id(), kRequestId);
  EXPECT_EQ(rep_response.tier(), AnnaTier::AMEMORY);
  EXPECT_EQ(rep_response.tuples().size(), 1);

  TxnKeyTuple rtp = rep_response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);
}

//////////////////////////////// LOG TESTS ////////////////////////////////

TEST_F(ServerHandlerTest, LogPrepareRequestResponse) {
  kSelfTier = Tier::LOG;
  Key key = "key";
  string value = "value";
  AnnaError error = AnnaError::NO_ERROR;

  stored_key_map[key].lock_ = 0;
  pending_requests[key].push_back( // storage pending_requests indexed on key
    PendingTxnRequest(RequestType::PREPARE_TXN, kTxnId, key, value,
                      UserThread(ip, 0).response_connect_address(), kRequestId));

  TxnResponse response;
  response.set_type(RequestType::PREPARE_TXN);
  response.set_response_id(kRequestId);
  response.set_tier(AnnaTier::AMEMORY);
  response.set_txn_id(kTxnId);

  TxnKeyTuple *tp = response.add_tuples();
  tp->set_key(key);
  tp->set_error(AnnaError::NO_ERROR);
  tp->set_payload(value);

  string prepare_response;
  response.SerializeToString(&prepare_response);

  unsigned access_count = 0;
  unsigned seed = 0;

  request_response_handler(seed, access_count, log_, prepare_response, global_hash_rings,
                           local_hash_rings, pending_requests, // pending_gossip,
                           key_access_tracker, stored_key_map, key_replication_map,
                           local_changeset, wt, txn_serializer, base_serializer,
                           log_serializer, pushers);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  TxnResponse rep_response;
  rep_response.ParseFromString(messages[0]);

  EXPECT_EQ(rep_response.txn_id(), kTxnId);
  EXPECT_EQ(rep_response.response_id(), kRequestId);
  EXPECT_EQ(rep_response.tier(), AnnaTier::ALOG);
  EXPECT_EQ(rep_response.tuples().size(), 1);

  TxnKeyTuple rtp = rep_response.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(pending_requests.size(), 0);
  EXPECT_EQ(log_serializer->size(), 1);
}
