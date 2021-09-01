import boto3
import subprocess

TXN_NODE_INSTANCE_ID = 'i-096b191e5f3c0e94e'
MEM_NODE_INSTANCE_ID = 'i-02a968d7be0607695'
LOG_NODE_INSTANCE_ID = 'i-005f739d8615dd7d2'

TXN_THREADS=1
MEM_THREADS=6
LOG_THREADS=6
BENCHMARK_THREADS=6

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

def generate_config(txn_public_ip, txn_private, public_ip, private_ip, is_txn=False):
  server_name = 'txn-server' if is_txn else 'server'
  return f"""
monitoring:
  mgmt_ip: {txn_public_ip}
  ip: {txn_public_ip}
routing:
  monitoring:
      - {txn_public_ip}
  ip: {txn_public_ip}
user:
  monitoring:
      - {txn_public_ip}
  routing:
      - {txn_public_ip}
  ip: {txn_public_ip}
{server_name}:
  monitoring:
      - {txn_private}
  routing:
      - {txn_private}
  seed_ip: {txn_private}
  public_ip: {public_ip}
  private_ip: {private_ip}
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
  txn: {TXN_THREADS}
  memory: {MEM_THREADS}
  ebs: 1
  log: {LOG_THREADS}
  routing: 1
  benchmark: {BENCHMARK_THREADS}
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

txn_config = generate_config(get_private_ip(txn_instance), get_private_ip(txn_instance), get_private_ip(txn_instance), get_private_ip(txn_instance), True)
mem_config = generate_config(get_public_ip(txn_instance), get_private_ip(txn_instance), get_public_ip(mem_instance), get_private_ip(mem_instance))
log_config = generate_config(get_public_ip(txn_instance), get_private_ip(txn_instance), get_public_ip(log_instance), get_private_ip(log_instance))

# Write the provided config into the instance's ~/anna/conf/anna-config.yml
def write_config(instance, config):
  cmd = ['ssh', '-o', 'StrictHostKeyChecking=accept-new', '-i', '~/west-region-key.pem', f'ubuntu@{get_public_ip(instance)}', 'cat - > anna/conf/anna-config.yml']
  p = subprocess.Popen(cmd, stdin=subprocess.PIPE)

  for chunki in range(0, len(config), 1024):
    chunk = config[chunki:chunki + 1024]
    p.stdin.write(bytearray(chunk, 'utf-8'))

write_config(txn_instance, txn_config)
write_config(mem_instance, mem_config)
write_config(log_instance, log_config)
