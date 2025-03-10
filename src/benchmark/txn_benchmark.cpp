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

#include <stdlib.h>

#include "benchmark.pb.h"
#include "client/kvs_client.hpp"
#include "client/txn_client.hpp"
#include "kvs_threads.hpp"
#include "yaml-cpp/yaml.h"

unsigned kBenchmarkThreadNum;
unsigned kRoutingThreadCount;
unsigned kDefaultLocalReplication;

ZmqUtil zmq_util;
ZmqUtilInterface *kZmqUtil = &zmq_util;

double get_base(unsigned N, double skew) {
  double base = 0;
  for (unsigned k = 1; k <= N; k++) {
    base += pow(k, -1 * skew);
  }
  return (1 / base);
}

double get_zipf_prob(unsigned rank, double skew, double base) {
  return pow(rank, -1 * skew) / base;
}

vector<TxnResponse> receive(TxnClientInterface *client) {
  vector<TxnResponse> responses = client->receive_txn_async();
  while (responses.size() == 0) {
    responses = client->receive_txn_async();
  }
  return responses;
}

int sample(int n, unsigned &seed, double base,
           map<unsigned, double> &sum_probs) {
  double z;           // Uniform random number (0 < z < 1)
  int zipf_value;     // Computed exponential value to be returned
  int i;              // Loop counter
  int low, high, mid; // Binary-search bounds

  // Pull a uniform random number (0 < z < 1)
  do {
    z = rand_r(&seed) / static_cast<double>(RAND_MAX);
  } while ((z == 0) || (z == 1));

  // Map z to the value
  low = 1, high = n;

  do {
    mid = floor((low + high) / 2);
    if (sum_probs[mid] >= z && sum_probs[mid - 1] < z) {
      zipf_value = mid;
      break;
    } else if (sum_probs[mid] >= z) {
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  } while (low <= high);

  // Assert that zipf_value is between 1 and N
  assert((zipf_value >= 1) && (zipf_value <= n));

  return zipf_value;
}

string generate_key(unsigned n) {
  return std::string(8 - std::to_string(n).length(), '0') + std::to_string(n);
}

void run(const unsigned &thread_id,
         const vector<UserRoutingThread> &routing_threads,
         const vector<MonitoringThread> &monitoring_threads,
         const Address &ip) {
  TxnClient client(routing_threads, ip, thread_id, 10000);
  string log_file = "blog_" + std::to_string(thread_id) + ".txt";
  string logger_name = "benchmark_log_" + std::to_string(thread_id);
  auto log = spdlog::basic_logger_mt(logger_name, log_file, true);
  log->flush_on(spdlog::level::info);

  client.set_logger(log);
  unsigned seed = client.get_seed();

  // observed per-key avg latency
  map<Key, std::pair<double, unsigned>> observed_latency;

  // responsible for pulling benchmark commands
  zmq::context_t &context = *(client.get_context());
  SocketCache pushers(&context, ZMQ_PUSH);
  zmq::socket_t command_puller(context, ZMQ_PULL);
  command_puller.bind("tcp://*:" +
                      std::to_string(thread_id + kBenchmarkCommandPort));

  vector<zmq::pollitem_t> pollitems = {
      {static_cast<void *>(command_puller), 0, ZMQ_POLLIN, 0}};

  auto client_id = std::to_string(thread_id);

  while (true) {
    kZmqUtil->poll(-1, &pollitems);

    if (pollitems[0].revents & ZMQ_POLLIN) {
      string msg = kZmqUtil->recv_string(&command_puller);
      // log->info("Received benchmark command: {}", msg);

      vector<string> v;
      split(msg, ':', v);
      string mode = v[0];

      unsigned num_keys = 1000;

      if (mode == "CACHE") {
        if (thread_id > 0) {
          // log->info("Received cache warming command, not thread 0");
        } else {
          client.clear_cache();
          auto warmup_start = std::chrono::system_clock::now();

          // create dummy txn for warming cache
          client.start_txn(client_id);

          // get txn id
          vector<TxnResponse> responses = receive(&client);
          auto txn_id = responses[0].txn_id();

          // warm up cache
          for (unsigned i = 1; i <= num_keys; i++) {
            // log->info("Warming up cache for key {}.", i);
            client.txn_put(client_id, txn_id, generate_key(i), "payload");
            receive(&client);
          }

          // commit dummy txn
          client.commit_txn(client_id, txn_id);
          receive(&client);

          auto warmup_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now() - warmup_start)
                                .count();
          log->info("Warming cache took {} ms", warmup_time);
        }
      } else if (mode == "TPS") {
        // To measure Transactions per second
        // TPS:<num_txns>:<zipf>

        unsigned num_txns = stod(v[1]);
        double zipf = stod(v[2]);
        unsigned gets_per_txn = 1;
        unsigned puts_per_txn = 1;
        auto payload = "payload";
        
        map<unsigned, double> sum_probs;
        double base;

        if (zipf > 0) {
          // log->info("Zipf coefficient is {}.", zipf);
          base = get_base(num_keys, zipf);
          sum_probs[0] = 0;

          for (unsigned i = 1; i <= num_keys; i++) {
            sum_probs[i] = sum_probs[i - 1] + base / pow((double)i, zipf);
          }
        } else {
          // log->info("Using a uniform random distribution.");
        }

        auto benchmark_start = std::chrono::system_clock::now();
        auto benchmark_end = std::chrono::system_clock::now();
        auto total_time = std::chrono::duration_cast<std::chrono::seconds>(
                              benchmark_end - benchmark_start)
                              .count();

        // map of txn index to (gets completed, puts completed, txn_id) tuple
        map<unsigned, std::tuple<unsigned, unsigned, string>> actions_done;

        // set of txn_ids that aborted
        std::set<std::string> aborted_txns = {};

        for (unsigned i = 0; i < num_txns; i++) {
          actions_done.insert(pair<unsigned, std::tuple<unsigned, unsigned, string>>(i, std::tuple<unsigned, unsigned, string>(0, 0, "")));
        }

        while (true) {
          if (actions_done.empty()) break;

          // choose a random txn index from the map
          auto it = actions_done.begin();
          std::advance(it, rand() % actions_done.size());
          unsigned i = it->first;
          unsigned& g = std::get<0>(it->second);
          unsigned& p = std::get<1>(it->second);
          string& txn_id = std::get<2>(it->second);

          if (txn_id.empty()) {
            // start this txn
            client.start_txn(client_id);

            // set txn id
            vector<TxnResponse> responses = receive(&client);
            txn_id = responses[0].txn_id();
            // log->info("[TPS] START TXN {}", txn_id);
          } else {
            // do an action for this txn
            // true for get, false for put
            bool type;

            if (g < gets_per_txn) {
              type = true;
            } else {
              type = false;
            }

            // get a random key
            unsigned k;
            if (zipf > 0) {
              k = sample(num_keys, seed, base, sum_probs);
            } else {
              k = rand_r(&seed) % (num_keys) + 1;
            }
            Key key = generate_key(k);

            if (type) {
              client.txn_get(client_id, txn_id, key);
              // log->info("[TPS] TXN GET {}, {}", txn_id, key);
            } else {
              client.txn_put(client_id, txn_id, key, payload);
              // log->info("[TPS] TXN PUT {}, {}", txn_id, key);
            }
            vector<TxnResponse> responses = receive(&client);
            TxnKeyTuple tuple = responses[0].tuples(0);

            if (tuple.error() == AnnaError::FAILED_OP) {
              // txn aborted, add to set
              aborted_txns.insert(txn_id);
            }

            if (type) {
              g++;
            } else {
              p++;
            }

            if (g >= gets_per_txn && p >= puts_per_txn) {
              // log->info("[TPS] COMMIT TXN {}", txn_id);

              // finished last action for this txn, commit
              client.commit_txn(client_id, txn_id);
              receive(&client);

              actions_done.erase(it);
            }
          }
        }

        benchmark_end = std::chrono::system_clock::now();
        total_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                          benchmark_end - benchmark_start)
                          .count();
        log->info("{}\t{}\t{}", num_txns, total_time, aborted_txns.size());

        UserFeedback feedback;

        feedback.set_uid(ip + ":" + std::to_string(thread_id));
        feedback.set_finish(true);

        string serialized_latency;
        feedback.SerializeToString(&serialized_latency);

        for (const MonitoringThread &thread : monitoring_threads) {
          kZmqUtil->send_string(
              serialized_latency,
              &pushers[thread.feedback_report_connect_address()]);
        }
      } else {
        log->info("{} is an invalid mode.", mode);
      }
    }
  }
}

int main(int argc, char *argv[]) {
  if (argc != 1) {
    std::cerr << "Usage: " << argv[0] << std::endl;
    return 1;
  }

  // read the YAML conf
  YAML::Node conf = YAML::LoadFile("conf/anna-config.yml");
  YAML::Node user = conf["user"];
  Address ip = user["ip"].as<string>();

  vector<MonitoringThread> monitoring_threads;
  vector<Address> routing_ips;

  YAML::Node monitoring = user["monitoring"];
  for (const YAML::Node &node : monitoring) {
    monitoring_threads.push_back(MonitoringThread(node.as<Address>()));
  }

  YAML::Node threads = conf["threads"];
  kRoutingThreadCount = threads["routing"].as<int>();
  kBenchmarkThreadNum = threads["benchmark"].as<int>();
  kDefaultLocalReplication = conf["replication"]["local"].as<unsigned>();

  vector<std::thread> benchmark_threads;

  if (YAML::Node elb = user["routing-elb"]) {
    routing_ips.push_back(elb.as<string>());
  } else {
    YAML::Node routing = user["routing"];

    for (const YAML::Node &node : routing) {
      routing_ips.push_back(node.as<Address>());
    }
  }

  vector<UserRoutingThread> routing_threads;
  for (const Address &ip : routing_ips) {
    for (unsigned i = 0; i < kRoutingThreadCount; i++) {
      routing_threads.push_back(UserRoutingThread(ip, i));
    }
  }

  // NOTE: We create a new client for every single thread.
  for (unsigned thread_id = 1; thread_id < kBenchmarkThreadNum; thread_id++) {
    benchmark_threads.push_back(
        std::thread(run, thread_id, routing_threads, monitoring_threads, ip));
  }

  run(0, routing_threads, monitoring_threads, ip);
}
