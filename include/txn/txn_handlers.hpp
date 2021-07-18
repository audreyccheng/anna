#ifndef INCLUDE_TXN_TXN_HANDLERS_HPP_
#define INCLUDE_TXN_TXN_HANDLERS_HPP_

#include "hash_ring.hpp"
#include "metadata.pb.h"
#include "requests.hpp"
#include "kvs/server_utils.hpp" // TODO(@accheng): refactor this
#include "txn_utils.hpp"

void node_join_handler(unsigned thread_id, unsigned &seed, Address public_ip,
                       Address private_ip, logger log, string &serialized,
                       GlobalRingMap &global_hash_rings,
                       LocalRingMap &local_hash_rings,
                       map<Key, KeyProperty> &stored_key_map,
                       map<Key, KeyReplication> &key_replication_map,
                       set<Key> &join_remove_set, SocketCache &pushers,
                       ServerThread &wt, AddressKeysetMap &join_gossip_map,
                       int self_join_count);

void node_depart_handler(unsigned thread_id, Address public_ip,
                         Address private_ip, GlobalRingMap &global_hash_rings,
                         logger log, string &serialized, SocketCache &pushers);

void self_depart_handler(unsigned thread_id, unsigned &seed, Address public_ip,
                         Address private_ip, logger log, string &serialized,
                         GlobalRingMap &global_hash_rings,
                         LocalRingMap &local_hash_rings,
                         map<Key, KeyProperty> &stored_key_map,
                         map<Key, KeyReplication> &key_replication_map,
                         vector<Address> &routing_ips,
                         vector<Address> &monitoring_ips, ServerThread &wt,
                         SocketCache &pushers, SerializerMap &serializers);

void user_txn_request_handler(
    unsigned &access_count, unsigned &seed, string &serialized, logger log,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<string, vector<PendingTxnRequest>> &pending_requests,
    map<Key, std::multiset<TimePoint>> &key_access_tracker,
    map<Key, TxnKeyProperty> &stored_txn_map,
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, TxnSerializer *serializer, SocketCache &pushers);

void storage_request_handler(
    unsigned &access_count, unsigned &seed, string &serialized, logger log,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<Key, vector<PendingTxnRequest>> &pending_requests,
    map<Key, std::multiset<TimePoint>> &key_access_tracker,
    map<Key, TxnKeyProperty> &stored_txn_map, // TODO(@accheng): update
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, BaseSerializer *serializer,
    SocketCache &pushers);

void gossip_handler(unsigned &seed, string &serialized,
                    GlobalRingMap &global_hash_rings,
                    LocalRingMap &local_hash_rings,
                    map<Key, vector<PendingGossip>> &pending_gossip,
                    map<Key, KeyProperty> &stored_key_map, 
                    map<Key, KeyReplication> &key_replication_map,
                    ServerThread &wt, SerializerMap &serializers,
                    SocketCache &pushers, logger log);

void log_request_handler(
    unsigned &access_count, unsigned &seed, string &serialized, logger log,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<Key, vector<PendingTxnRequest>> &pending_requests, 
    map<Key, std::multiset<TimePoint>> &key_access_tracker,
    map<Key, TxnKeyProperty> &stored_txn_map, // TODO(@accheng): update
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, LogSerializer *serializer,
    SocketCache &pushers);

void request_response_handler(
    unsigned &seed, unsigned &access_count, logger log, string &serialized,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<Key, vector<PendingTxnRequest>> &pending_requests,
    // map<Key, vector<PendingGossip>> &pending_gossip,
    map<Key, std::multiset<TimePoint>> &key_access_tracker,
    map<Key, TxnKeyProperty> &stored_txn_map,
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, TxnSerializer *txn_serializer, BaseSerializer *base_serializer, 
    LogSerializer *log_serializer, SocketCache &pushers);

void replication_response_handler(
    unsigned &seed, unsigned &access_count, logger log, string &serialized,
    GlobalRingMap &global_hash_rings, LocalRingMap &local_hash_rings,
    map<Key, vector<PendingTxnRequest>> &pending_requests,
    // map<Key, vector<PendingGossip>> &pending_gossip,
    map<Key, std::multiset<TimePoint>> &key_access_tracker,
    map<Key, TxnKeyProperty> &stored_txn_map,
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, TxnSerializer *txn_serializer, BaseSerializer *base_serializer, 
    LogSerializer *log_serializer, SocketCache &pushers);

void replication_change_handler(
    Address public_ip, Address private_ip, unsigned thread_id, unsigned &seed,
    logger log, string &serialized, GlobalRingMap &global_hash_rings,
    LocalRingMap &local_hash_rings, map<Key, KeyProperty> &stored_key_map,
    map<Key, KeyReplication> &key_replication_map, set<Key> &local_changeset,
    ServerThread &wt, SerializerMap &serializers, SocketCache &pushers);

// Postcondition:
// cache_ip_to_keys, key_to_cache_ips are both updated
// with the IPs and their fresh list of repsonsible keys
// in the serialized response.
void cache_ip_response_handler(string &serialized,
                               map<Address, set<Key>> &cache_ip_to_keys,
                               map<Key, set<Address>> &key_to_cache_ips);

void management_node_response_handler(string &serialized,
                                      set<Address> &extant_caches,
                                      map<Address, set<Key>> &cache_ip_to_keys,
                                      map<Key, set<Address>> &key_to_cache_ips,
                                      GlobalRingMap &global_hash_rings,
                                      LocalRingMap &local_hash_rings,
                                      SocketCache &pushers, ServerThread &wt,
                                      unsigned &rid);

std::pair<string, AnnaError> process_get(const Key &key,
                                         Serializer *serializer);

void process_put(const Key &key, LatticeType lattice_type,
                 const string &payload, Serializer *serializer,
                 map<Key, KeyProperty> &stored_key_map);

void send_gossip(AddressKeysetMap &addr_keyset_map, SocketCache &pushers,
                 SerializerMap &serializers,
                 map<Key, KeyProperty> &stored_key_map);

string process_start_txn(const string &client_id, TxnSerializer *serializer,
                       map<Key, TxnKeyProperty> &stored_txn_map);

void process_put_start_txn(const string &txn_id, TxnSerializer *serializer,
                       map<Key, TxnKeyProperty> &stored_txn_map);

void process_put_op(const string &txn_id, const Key &key,
                    const string &payload, AnnaError &error,
                    TxnSerializer *serializer,
                    map<Key, TxnKeyProperty> &stored_txn_map);

vector<Operation> process_get_ops(
          const string &txn_id, AnnaError &error,
          TxnSerializer *serializer,
          map<Key, TxnKeyProperty> &stored_txn_map);

void process_commit_txn(const string &txn_id, AnnaError &error, 
                        TxnSerializer *serializer,
                        map<Key, TxnKeyProperty> &stored_txn_map);

string process_txn_get(const string &txn_id, const Key &key, 
                       AnnaError &error, BaseSerializer *serializer, 
                       map<Key, TxnKeyProperty> &stored_key_map);

void process_txn_put(const string &txn_id, const Key &key,
                     const string &payload, AnnaError &error,
                     const bool &is_primary, BaseSerializer *serializer,
                     map<Key, TxnKeyProperty> &stored_key_map);

void process_txn_prepare(const string &txn_id, const Key &key,
                         AnnaError &error, BaseSerializer *serializer,
                         map<Key, TxnKeyProperty> &stored_key_map);

void process_txn_commit(const string &txn_id, const Key &key,
                        AnnaError &error, BaseSerializer *serializer,
                        map<Key, TxnKeyProperty> &stored_key_map);

void process_log(const string &txn_id, const Key &key,
                 const string &payload, AnnaError &error,
                 LogSerializer *serializer);

bool is_primary_replica(const Key &key,
                        map<Key, KeyReplication> &key_replication_map,
                        GlobalRingMap &global_hash_rings,
                        LocalRingMap &local_hash_rings, ServerThread &st);

bool should_abort(AnnaError error);

#endif // INCLUDE_TXN_TXN_HANDLERS_HPP_