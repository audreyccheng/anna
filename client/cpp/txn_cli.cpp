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

#include <fstream>

#include "client/txn_client.hpp"
#include "yaml-cpp/yaml.h"

#include <assert.h>
#include <string>

unsigned kRoutingThreadCount;

ZmqUtil zmq_util;
ZmqUtilInterface *kZmqUtil = &zmq_util;

void print_set(set<string> set) {
  std::cout << "{ ";
  for (const string &val : set) {
    std::cout << val << " ";
  }

  std::cout << "}" << std::endl;
}

void handle_request(KvsClientInterface *client, string input) {
  vector<string> v;
  split(input, ' ', v);

  if (v.size() == 0) {
    std::exit(EXIT_SUCCESS);
  }

  if (v[0] == "START_TXN") {
    auto client_id = "0";
    auto id = v[1];
    uint64_t txn_id = std::stoi(client_id + id);
    client->start_txn(txn_id);

    vector<TxnResponse> responses = client->receive_txn_async();
    while (responses.size() == 0) {
      responses = client->receive_txn_async();
    }

    if (responses.size() > 1) {
      std::cout << "Error: received more than one response" << std::endl;
    }

    std::cout << responses[0].response() << std::endl;
  }
}

void run(KvsClientInterface *client) {
  string input;
  while (true) {
    std::cout << "txn-kvs> ";

    getline(std::cin, input);
    handle_request(client, input);
  }
}

void run(KvsClientInterface *client, string filename) {
  string input;
  std::ifstream infile(filename);

  while (getline(infile, input)) {
    handle_request(client, input);
  }
}

int main(int argc, char *argv[]) {
  if (argc < 2 || argc > 3) {
    std::cerr << "Usage: " << argv[0] << " conf-file <input-file>" << std::endl;
    std::cerr
        << "Filename is optional. Omit the filename to run in interactive mode."
        << std::endl;
    return 1;
  }

  // read the YAML conf
  YAML::Node conf = YAML::LoadFile(argv[1]);
  kRoutingThreadCount = conf["threads"]["routing"].as<unsigned>();

  YAML::Node user = conf["user"];
  Address ip = user["ip"].as<Address>();

  vector<Address> routing_ips;
  if (YAML::Node elb = user["routing-elb"]) {
    routing_ips.push_back(elb.as<string>());
  } else {
    YAML::Node routing = user["routing"];
    for (const YAML::Node &node : routing) {
      routing_ips.push_back(node.as<Address>());
    }
  }

  vector<UserRoutingThread> threads;
  for (Address addr : routing_ips) {
    for (unsigned i = 0; i < kRoutingThreadCount; i++) {
      threads.push_back(UserRoutingThread(addr, i));
    }
  }

  KvsClient client(threads, ip, 0, 10000);

  if (argc == 2) {
    run(&client);
  } else {
    run(&client, argv[2]);
  }
}
