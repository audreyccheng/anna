import boto3
import subprocess

TXN_NODE_INSTANCE_ID = 'i-096b191e5f3c0e94e'
MEM_NODE_INSTANCE_ID = 'i-02a968d7be0607695'
LOG_NODE_INSTANCE_ID = 'i-005f739d8615dd7d2'

ec2 = boto3.client('ec2')

def get_instance(r): return r['Instances'][0]

def get_id(i): return i['InstanceId']

def get_private_ip(i): return i['PrivateIpAddress']

def get_public_ip(i): return i['PublicIpAddress']

def get_instance_by_id(instances, id): 
  return list(filter(lambda i: get_id(i) == id, instances))[0]

reservations = ec2.describe_instances()['Reservations']
instances = list(map(get_instance, reservations))

txn_instance = get_instance_by_id(instances, TXN_NODE_INSTANCE_ID)
mem_instance = get_instance_by_id(instances, MEM_NODE_INSTANCE_ID)
log_instance = get_instance_by_id(instances, LOG_NODE_INSTANCE_ID)

txn_config = f"""
monitoring:
  mgmt_ip: {get_private_ip(txn_instance)}
  ip: {get_private_ip(txn_instance)}
routing:
  monitoring:
      - {get_private_ip(txn_instance)}
  ip: {get_private_ip(txn_instance)}
user:
  monitoring:
      - {get_private_ip(txn_instance)}
  routing:
      - {get_private_ip(txn_instance)}
  ip: {get_private_ip(txn_instance)}
txn-server:
  monitoring:
      - {get_private_ip(txn_instance)}
  routing:
      - {get_private_ip(txn_instance)}
  seed_ip: {get_private_ip(txn_instance)}
  public_ip: {get_private_ip(txn_instance)}
  private_ip: {get_private_ip(txn_instance)}
  mgmt_ip: "NULL"
policy:
  elasticity: false
  selective-rep: false
  tiering: true
ebs: ./
capacities: # in GB
  txn-cap: 1
  memory-cap: 1
  ebs-cap: 0
  log-cap: 1
threads:
  txn: 1
  memory: 6
  ebs: 1
  log: 6
  routing: 1
  benchmark: 6
replication:
  txn: 1
  memory: 1
  ebs: 0
  log: 1
  minimum: 1
  local: 1
benchmark:
    - localhost
"""

# mem_config = f"""
# monitoring:
#   mgmt_ip: {get_public_ip(txn_instance)}
#   ip: {get_public_ip(txn_instance)}
# routing:
#   monitoring:
#       - {get_public_ip(txn_instance)}
#   ip: {get_public_ip(txn_instance)}
# user:
#   monitoring:
#       - {get_public_ip(txn_instance)}
#   routing:
#       - {get_public_ip(txn_instance)}
#   ip: {get_public_ip(txn_instance)}
# server:
#   monitoring:
#       - {get_private_ip(txn_instance)}
#   routing:
#       - {get_private_ip(txn_instance)}
#   seed_ip: {get_private_ip(txn_instance)}
#   public_ip: {get_public_ip(mem_instance)}
#   private_ip: {get_private_ip(mem_instance)}
#   mgmt_ip: "NULL"
# policy:
#   elasticity: false
#   selective-rep: false
#   tiering: false
# ebs: ./
# capacities: # in GB
#   txn-cap: 1
#   memory-cap: 1
#   ebs-cap: 0
#   log-cap: 1
# threads:
#   txn: 1
#   memory: 6
#   ebs: 1
#   log: 6
#   routing: 1
#   benchmark: 6
# replication:
#   txn: 1
#   memory: 1
#   ebs: 0
#   log: 1
#   minimum: 1
#   local: 1
# """

# log_config = f"""
# monitoring:
#   mgmt_ip: {get_public_ip(txn_instance)}
#   ip: {get_public_ip(txn_instance)}
# routing:
#   monitoring:
#       - {get_public_ip(txn_instance)}
#   ip: {get_public_ip(txn_instance)}
# user:
#   monitoring:
#       - {get_public_ip(txn_instance)}
#   routing:
#       - {get_public_ip(txn_instance)}
#   ip: {get_public_ip(txn_instance)}
# server:
#   monitoring:
#       - {get_private_ip(txn_instance)}
#   routing:
#       - {get_private_ip(txn_instance)}
#   seed_ip: {get_private_ip(txn_instance)}
#   public_ip: {get_public_ip(log_instance)}
#   private_ip: {get_private_ip(log_instance)}
#   mgmt_ip: "NULL"
# policy:
#   elasticity: false
#   selective-rep: false
#   tiering: false
# ebs: ./
# capacities: # in GB
#   txn-cap: 1
#   memory-cap: 1
#   ebs-cap: 0
#   log-cap: 1
# threads:
#   txn: 1
#   memory: 6
#   ebs: 1
#   log: 6
#   routing: 1
#   benchmark: 6
# replication:
#   txn: 1
#   memory: 1
#   ebs: 0
#   log: 1
#   minimum: 1
#   local: 1
# """

# SSH into all 3 instances and write these configs into the config files
cmd = ['ssh', '-i', '~/west-region-key.pem', f'ubuntu@{get_public_ip(txn_instance)}', 'cat - > anna/conf/anna-txn.yml']
p = subprocess.Popen(cmd, stdin=subprocess.PIPE)

for chunki in range(0, len(txn_config), 1024):
  chunk = txn_config[chunki:chunki + 1024]
  p.stdin.write(bytearray(chunk, 'utf-8'))
