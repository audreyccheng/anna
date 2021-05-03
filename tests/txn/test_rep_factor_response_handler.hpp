#include "txn/txn_handlers.hpp"

//////////////////////////////// TXN TESTS ////////////////////////////////

TEST_F(ServerHandlerTest, TxnReplicationResponse) {
  kSelfTier = Tier::TXN;
  unsigned access_count = 0;
  unsigned seed = 0;

  // Test replication of existing txn
  // string key = "key";
  string key = kTxnId;
  vector<string> keys = {kTxnId};
  warmup_key_replication_map_to_defaults(keys);

  EXPECT_EQ(key_replication_map[key].global_replication_[Tier::TXN], 1);
  EXPECT_EQ(key_replication_map[key].global_replication_[Tier::MEMORY], 1);
  EXPECT_EQ(key_replication_map[key].global_replication_[Tier::DISK], 1);
  // EXPECT_EQ(key_replication_map[key].global_replication_[Tier::LOG], 1);
  EXPECT_EQ(key_replication_map[key].local_replication_[Tier::TXN], 1);
  EXPECT_EQ(key_replication_map[key].local_replication_[Tier::MEMORY], 1);
  EXPECT_EQ(key_replication_map[key].local_replication_[Tier::DISK], 1);
  // EXPECT_EQ(key_replication_map[key].local_replication_[Tier::LOG], 1);

  TxnResponse response;
  response.set_type(RequestType::START_TXN);
  response.set_tier(AnnaTier::ATXN);
  response.set_txn_id(kTxnId);
  TxnKeyTuple *tp = response.add_tuples();
  tp->set_key(get_metadata_key(key, MetadataType::replication));
  tp->set_error(AnnaError::NO_ERROR);

  string metakey = key;
  ReplicationFactor rf;
  rf.set_key(key);

  for (const Tier &tier : kAllTiers) {
    ReplicationFactor_ReplicationValue *rep_global = rf.add_global();
    rep_global->set_tier(tier);
    rep_global->set_value(2);

    ReplicationFactor_ReplicationValue *rep_local = rf.add_local();
    rep_local->set_tier(tier);
    rep_local->set_value(3);
  }

  string repfactor;
  rf.SerializeToString(&repfactor);

  tp->set_payload(repfactor);

  string serialized;
  response.SerializeToString(&serialized);

  replication_response_handler(seed, access_count, log_, serialized, global_hash_rings,
                               local_hash_rings, pending_requests, // pending_gossip,
                               key_access_tracker, stored_key_map, key_replication_map,
                               local_changeset, wt, txn_serializer, base_serializer,
                               log_serializer, pushers);

  EXPECT_EQ(key_replication_map[key].global_replication_[Tier::TXN], 2);
  EXPECT_EQ(key_replication_map[key].global_replication_[Tier::MEMORY], 2);
  EXPECT_EQ(key_replication_map[key].global_replication_[Tier::DISK], 2);
  // EXPECT_EQ(key_replication_map[key].global_replication_[Tier::LOG], 2);
  EXPECT_EQ(key_replication_map[key].local_replication_[Tier::TXN], 3);
  EXPECT_EQ(key_replication_map[key].local_replication_[Tier::MEMORY], 3);
  EXPECT_EQ(key_replication_map[key].local_replication_[Tier::DISK], 3);
  // EXPECT_EQ(key_replication_map[key].local_replication_[Tier::LOG], 3);


  // // Test replication of new key
  string new_key = "new_key";

  TxnResponse new_response;
  new_response.set_type(RequestType::START_TXN);
  new_response.set_tier(AnnaTier::ATXN);
  new_response.set_txn_id(new_key);
  TxnKeyTuple *new_tp = new_response.add_tuples();
  new_tp->set_key(get_metadata_key(new_key, MetadataType::replication));
  new_tp->set_error(AnnaError::KEY_DNE);

  string new_serialized;
  new_response.SerializeToString(&new_serialized);

  replication_response_handler(seed, access_count, log_, new_serialized, global_hash_rings,
                               local_hash_rings, pending_requests, // pending_gossip,
                               key_access_tracker, stored_key_map, key_replication_map,
                               local_changeset, wt, txn_serializer, base_serializer,
                               log_serializer, pushers);

  EXPECT_EQ(key_replication_map[new_key].global_replication_[Tier::TXN], 1);
  EXPECT_EQ(key_replication_map[new_key].global_replication_[Tier::MEMORY], 0);
  EXPECT_EQ(key_replication_map[new_key].global_replication_[Tier::DISK], 0);
  // EXPECT_EQ(key_replication_map[key].global_replication_[Tier::LOG], 0);
  EXPECT_EQ(key_replication_map[new_key].local_replication_[Tier::TXN], 1);
  EXPECT_EQ(key_replication_map[new_key].local_replication_[Tier::MEMORY], 0);
  EXPECT_EQ(key_replication_map[new_key].local_replication_[Tier::DISK], 0);
  // EXPECT_EQ(key_replication_map[key].local_replication_[Tier::LOG], 0);
}

TEST_F(ServerHandlerTest, StorageReplicationResponse) {
  kSelfTier = Tier::MEMORY;
  unsigned access_count = 0;
  unsigned seed = 0;

  // Test replication of existing txn
  string key = "key";
  vector<string> keys = {key};
  warmup_key_replication_map_to_defaults(keys);

  EXPECT_EQ(key_replication_map[key].global_replication_[Tier::TXN], 1);
  EXPECT_EQ(key_replication_map[key].global_replication_[Tier::MEMORY], 1);
  EXPECT_EQ(key_replication_map[key].global_replication_[Tier::DISK], 1);
  // EXPECT_EQ(key_replication_map[key].global_replication_[Tier::LOG], 1);
  EXPECT_EQ(key_replication_map[key].local_replication_[Tier::TXN], 1);
  EXPECT_EQ(key_replication_map[key].local_replication_[Tier::MEMORY], 1);
  EXPECT_EQ(key_replication_map[key].local_replication_[Tier::DISK], 1);
  // EXPECT_EQ(key_replication_map[key].local_replication_[Tier::LOG], 1);

  TxnResponse response;
  response.set_type(RequestType::TXN_GET);
  response.set_tier(AnnaTier::AMEMORY);
  response.set_txn_id(kTxnId);
  TxnKeyTuple *tp = response.add_tuples();
  tp->set_key(get_metadata_key(key, MetadataType::replication));
  tp->set_error(AnnaError::NO_ERROR);

  string metakey = key;
  ReplicationFactor rf;
  rf.set_key(key);

  for (const Tier &tier : kAllTiers) {
    ReplicationFactor_ReplicationValue *rep_global = rf.add_global();
    rep_global->set_tier(tier);
    rep_global->set_value(2);

    ReplicationFactor_ReplicationValue *rep_local = rf.add_local();
    rep_local->set_tier(tier);
    rep_local->set_value(3);
  }

  string repfactor;
  rf.SerializeToString(&repfactor);

  tp->set_payload(repfactor);

  string serialized;
  response.SerializeToString(&serialized);

  replication_response_handler(seed, access_count, log_, serialized, global_hash_rings,
                               local_hash_rings, pending_requests, // pending_gossip,
                               key_access_tracker, stored_key_map, key_replication_map,
                               local_changeset, wt, txn_serializer, base_serializer,
                               log_serializer, pushers);

  EXPECT_EQ(key_replication_map[key].global_replication_[Tier::TXN], 2);
  EXPECT_EQ(key_replication_map[key].global_replication_[Tier::MEMORY], 2);
  EXPECT_EQ(key_replication_map[key].global_replication_[Tier::DISK], 2);
  // EXPECT_EQ(key_replication_map[key].global_replication_[Tier::LOG], 2);
  EXPECT_EQ(key_replication_map[key].local_replication_[Tier::TXN], 3);
  EXPECT_EQ(key_replication_map[key].local_replication_[Tier::MEMORY], 3);
  EXPECT_EQ(key_replication_map[key].local_replication_[Tier::DISK], 3);
  // EXPECT_EQ(key_replication_map[key].local_replication_[Tier::LOG], 3);


  // Test replication of new key
  string new_key = "new_key";

  TxnResponse new_response;
  new_response.set_type(RequestType::TXN_GET);
  new_response.set_tier(AnnaTier::AMEMORY);
  new_response.set_txn_id(kTxnId);
  TxnKeyTuple *new_tp = new_response.add_tuples();
  new_tp->set_key(get_metadata_key(new_key, MetadataType::replication));
  new_tp->set_error(AnnaError::KEY_DNE);

  string new_serialized;
  new_response.SerializeToString(&new_serialized);

  replication_response_handler(seed, access_count, log_, new_serialized, global_hash_rings,
                               local_hash_rings, pending_requests, // pending_gossip,
                               key_access_tracker, stored_key_map, key_replication_map,
                               local_changeset, wt, txn_serializer, base_serializer,
                               log_serializer, pushers);

  EXPECT_EQ(key_replication_map[new_key].global_replication_[Tier::TXN], 0);
  EXPECT_EQ(key_replication_map[new_key].global_replication_[Tier::MEMORY], 1);
  EXPECT_EQ(key_replication_map[new_key].global_replication_[Tier::DISK], 0);
  // EXPECT_EQ(key_replication_map[key].global_replication_[Tier::LOG], 0);
  EXPECT_EQ(key_replication_map[new_key].local_replication_[Tier::TXN], 0);
  EXPECT_EQ(key_replication_map[new_key].local_replication_[Tier::MEMORY], 1);
  EXPECT_EQ(key_replication_map[new_key].local_replication_[Tier::DISK], 0);
  // EXPECT_EQ(key_replication_map[key].local_replication_[Tier::LOG], 0);
}

TEST_F(ServerHandlerTest, TxnStartReplicationResponse) {
  kSelfTier = Tier::TXN;
  string client_id = "0";
  AnnaError error = AnnaError::NO_ERROR;

  // Assume this client has already sent requests before
  stored_key_map[client_id].num_ops_ = 0;
  pending_requests[client_id].push_back(
  	PendingTxnRequest(RequestType::START_TXN, client_id, client_id, "",
                      UserThread(ip, 0).response_connect_address(), kRequestId));

  TxnResponse response;
  response.set_type(RequestType::START_TXN);
  response.set_response_id(kRequestId);
  response.set_tier(AnnaTier::ATXN);

  TxnKeyTuple *tp = response.add_tuples();
  tp->set_key(get_metadata_key(client_id, MetadataType::replication));

  string start_response;
  response.SerializeToString(&start_response);

  unsigned access_count = 0;
  unsigned seed = 0;

  replication_response_handler(seed, access_count, log_, start_response, global_hash_rings,
                               local_hash_rings, pending_requests, // pending_gossip,
                               key_access_tracker, stored_key_map, key_replication_map,
                               local_changeset, wt, txn_serializer, base_serializer,
                               log_serializer, pushers);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  TxnResponse rep_response;
  rep_response.ParseFromString(messages[0]);
  string txn_id = rep_response.txn_id();

  EXPECT_NE(txn_id, "");
  EXPECT_NE(stored_key_map.find(txn_id), stored_key_map.end());
  EXPECT_EQ(rep_response.response_id(), kRequestId);
  EXPECT_EQ(rep_response.tier(), AnnaTier::ATXN);
  EXPECT_EQ(rep_response.tuples().size(), 1);

  TxnKeyTuple rtp = rep_response.tuples(0);

  EXPECT_EQ(rtp.key(), client_id);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(pending_requests.size(), 0);
}

TEST_F(ServerHandlerTest, TxnGetReplicationResponse) {
  kSelfTier = Tier::TXN;
  string txn_id = "0:0";
  Key key = "key";
  AnnaError error = AnnaError::NO_ERROR;

  // Assume this txn exists
  stored_key_map[txn_id].num_ops_ = 0;
  pending_requests[txn_id].push_back(
  	PendingTxnRequest(RequestType::TXN_GET, txn_id, key, "",
                      UserThread(ip, 0).response_connect_address(), kRequestId));

  TxnResponse response;
  response.set_type(RequestType::TXN_GET);
  response.set_response_id(kRequestId);
  response.set_tier(AnnaTier::ATXN);
  response.set_txn_id(txn_id);

  TxnKeyTuple *tp = response.add_tuples();
  tp->set_key(key);
  tp->set_error(AnnaError::NO_ERROR);

  string get_response;
  response.SerializeToString(&get_response);

  unsigned access_count = 0;
  unsigned seed = 0;

  replication_response_handler(seed, access_count, log_, get_response, global_hash_rings,
                               local_hash_rings, pending_requests, // pending_gossip,
                               key_access_tracker, stored_key_map, key_replication_map,
                               local_changeset, wt, txn_serializer, base_serializer,
                               log_serializer, pushers);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  // check that request to storage_request_handler is well-formed
  TxnRequest request;
  request.ParseFromString(messages[0]);
  string req_rep_addr = "tcp://127.0.0.1:6260";

  EXPECT_EQ(request.txn_id(), txn_id);
  EXPECT_EQ(request.type(), RequestType::TXN_GET);
  EXPECT_EQ(request.response_address(), req_rep_addr);
  EXPECT_EQ(request.tuples().size(), 1);

  TxnKeyTuple rtp = request.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(pending_requests.size(), 1);
}

//////////////////////////////// STORAGE TESTS ////////////////////////////////

TEST_F(ServerHandlerTest, StorageGetReplicationResponse) {
  kSelfTier = Tier::MEMORY;
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

  pending_requests[key].push_back( // storage pending_requests indexed on key
  	PendingTxnRequest(RequestType::TXN_GET, kTxnId, key, "",
                      UserThread(ip, 0).response_connect_address(), kRequestId));

  TxnResponse response;
  response.set_type(RequestType::TXN_GET);
  response.set_response_id(kRequestId);
  response.set_tier(AnnaTier::AMEMORY);
  response.set_txn_id(kTxnId);

  TxnKeyTuple *tp = response.add_tuples();
  tp->set_key(key);
  tp->set_error(AnnaError::NO_ERROR);

  string get_response;
  response.SerializeToString(&get_response);

  unsigned access_count = 0;
  unsigned seed = 0;

  replication_response_handler(seed, access_count, log_, get_response, global_hash_rings,
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
  EXPECT_EQ(rtp.payload(), value);
  EXPECT_EQ(rtp.error(), 0);
}

TEST_F(ServerHandlerTest, StoragePutReplicationResponse) {
  kSelfTier = Tier::MEMORY;
  Key key = "key";
  string value = "value";
  AnnaError error = AnnaError::NO_ERROR;

  pending_requests[key].push_back( // storage pending_requests indexed on key
  	PendingTxnRequest(RequestType::TXN_PUT, kTxnId, key, value,
                      UserThread(ip, 0).response_connect_address(), kRequestId));

  TxnResponse response;
  response.set_type(RequestType::TXN_PUT);
  response.set_response_id(kRequestId);
  response.set_tier(AnnaTier::AMEMORY);
  response.set_txn_id(kTxnId);

  TxnKeyTuple *tp = response.add_tuples();
  tp->set_key(key);
  tp->set_error(AnnaError::NO_ERROR);

  string put_response;
  response.SerializeToString(&put_response);

  unsigned access_count = 0;
  unsigned seed = 0;

  replication_response_handler(seed, access_count, log_, put_response, global_hash_rings,
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

  EXPECT_EQ(local_changeset.size(), 1);

  // Ensure that lock was held by put request
  base_serializer->put(kTxnId, key, value, error);
  EXPECT_EQ(error, AnnaError::FAILED_OP);
}

TEST_F(ServerHandlerTest, StoragePrepareReplicationResponse) {
  kSelfTier = Tier::MEMORY;
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

  string prepare_response;
  response.SerializeToString(&prepare_response);

  unsigned access_count = 0;
  unsigned seed = 0;

  replication_response_handler(seed, access_count, log_, prepare_response, global_hash_rings,
                               local_hash_rings, pending_requests, // pending_gossip,
                               key_access_tracker, stored_key_map, key_replication_map,
                               local_changeset, wt, txn_serializer, base_serializer,
                               log_serializer, pushers);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  // check that request to log_request_handler is well-formed
  TxnRequest request;
  request.ParseFromString(messages[0]);
  string req_rep_addr = "tcp://127.0.0.1:6260";

  EXPECT_EQ(request.txn_id(), kTxnId);
  EXPECT_EQ(request.type(), RequestType::PREPARE_TXN);
  EXPECT_EQ(request.response_address(), req_rep_addr);
  EXPECT_EQ(request.tuples().size(), 1);

  TxnKeyTuple rtp = request.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(pending_requests.size(), 1);
}

TEST_F(ServerHandlerTest, StorageCommitReplicationResponse) {
  kSelfTier = Tier::MEMORY;
  Key key = "key";
  string value = "value";
  AnnaError error = AnnaError::NO_ERROR;

	stored_key_map[key].lock_ = 0;
  pending_requests[key].push_back( // storage pending_requests indexed on key
  	PendingTxnRequest(RequestType::COMMIT_TXN, kTxnId, key, value,
                      UserThread(ip, 0).response_connect_address(), kRequestId));

  TxnResponse response;
  response.set_type(RequestType::COMMIT_TXN);
  response.set_response_id(kRequestId);
  response.set_tier(AnnaTier::AMEMORY);
  response.set_txn_id(kTxnId);

  TxnKeyTuple *tp = response.add_tuples();
  tp->set_key(key);
  tp->set_error(AnnaError::NO_ERROR);

  string commit_response;
  response.SerializeToString(&commit_response);

  unsigned access_count = 0;
  unsigned seed = 0;

  replication_response_handler(seed, access_count, log_, commit_response, global_hash_rings,
                               local_hash_rings, pending_requests, // pending_gossip,
                               key_access_tracker, stored_key_map, key_replication_map,
                               local_changeset, wt, txn_serializer, base_serializer,
                               log_serializer, pushers);

  vector<string> messages = get_zmq_messages();
  EXPECT_EQ(messages.size(), 1);

  // check that request to log_request_handler is well-formed
  TxnRequest request;
  request.ParseFromString(messages[0]);
  string req_rep_addr = "tcp://127.0.0.1:6260";

  EXPECT_EQ(request.txn_id(), kTxnId);
  EXPECT_EQ(request.type(), RequestType::COMMIT_TXN);
  EXPECT_EQ(request.response_address(), req_rep_addr);
  EXPECT_EQ(request.tuples().size(), 1);

  TxnKeyTuple rtp = request.tuples(0);

  EXPECT_EQ(rtp.key(), key);
  EXPECT_EQ(rtp.error(), 0);

  EXPECT_EQ(pending_requests.size(), 1);
}

//////////////////////////////// LOG TESTS ////////////////////////////////

TEST_F(ServerHandlerTest, LogPrepareReplicationResponse) {
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

  replication_response_handler(seed, access_count, log_, prepare_response, global_hash_rings,
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
