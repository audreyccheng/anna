#!/bin/bash

#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

if [ -z "$1" ] && [ -z "$2" ]; then
  echo "Usage: ./$0 build start-user"
  echo ""
  echo "You must run this from the project root directory."
  exit 1
fi

if [ "$1" = "y" ] || [ "$1" = "yes" ]; then
  ./scripts/build.sh
fi

cp conf/anna-node-1.yml conf/anna-config.yml

./build/target/kvs/anna-monitor &
MPID=$!
./build/target/kvs/anna-route &
RPID=$!
# export SERVER_TYPE="memory"
# ./build/target/kvs/anna-txn &
# SPID=$!
# export SERVER_TYPE="txn"
# ./build/target/kvs/anna-txn &
# S2PID=$!

# export SERVER_TYPE="memory"
# ./build/target/kvs/anna-kvs &
# S3PID=$!

echo $MPID 
echo $RPID 
# echo $SPID 
# echo $S2PID
# echo $S3PID

echo $MPID >> pids
echo $RPID >> pids
# echo $SPID >> pids
# echo $S2PID >> pids
# echo $S3PID >> pids

if [ "$2" = "y" ] || [ "$2" = "yes" ]; then
  ./build/cli/anna-cli conf/anna-node-1.yml
fi

if [ "$2" = "t" ] || [ "$2" = "txn" ]; then
  ./build/cli/anna-txn-cli conf/anna-node-1.yml
fi
