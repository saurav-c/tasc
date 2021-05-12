import sys
sys.path.append('./../../cluster')
sys.path.append('./../../cluster/kops')

from util import load_yaml
import tools

import csv
import subprocess
import os
import time

CONFIG_FILE = './throughput/config.yml'
CLUSTER_CONFIG_FILE = './throughput/cluster_config.txt'
DATA_FILE = './throughput/data.csv'
BEST_DATA_FILE = './throughput/best_data.csv'

BASE_CLIENTS = 35
DEFAULT_LAMBDA = 'tasc-lambda'

def main():
    print('Loading config...')
    config = load_yaml(CONFIG_FILE)

    anna_manager_ip = config['anna']

    print('Loading cluster configurations...')
    lines = None
    with open(CLUSTER_CONFIG_FILE) as f:
        lines = [line.rstrip() for line in f]

    print('Creating data csv files...')
    dataFile = open(DATA_FILE, 'w+', newline='')
    dataWriter = csv.writer(dataFile)
    dataWriter.writerow(['Clients', 'Transaction Managers', 'Key Nodes', 'Workers', 'Throughput'])
    bestDataFile = open(BEST_DATA_FILE, 'w+', newline='')
    bestWriter = csv.writer(bestDataFile)
    bestWriter.writerow(['Transaction Managers', 'Key Nodes', 'Workers', 'Peak Throughput'])

    for i in range(len(lines)):
        cc = lines[i]
        split = cc.split(',')
        txn, key, worker = int(split[0]), int(split[1]), int(split[2])
        print('Initializing cluster for {} Txn Managers, {} Key Nodes, {} Workers'.format(txn, key, worker))
        toWarm = init(txn, key, worker, anna_manager_ip)
        if toWarm or i == 0:
            print('Warming up to deal with newly added nodes or (1st run)...')
            warmup(config)

        print('Running workload...')
        base_client = BASE_CLIENTS
        data = run(config, anna_manager_ip, base_clients=base_client)
        bestTPut = 0.0
        for x in data:
            clients, tput = x[0], x[1]
            bestTPut = max(bestTPut, x[1])
            dataWriter.writerow([clients, txn, key, worker, tput])
        bestWriter.writerow([txn, key, worker, bestTPut])
        dataFile.flush()
        bestDataFile.flush()
        print('Peak Throughput of {} for {} Txn Managers, {} Key Nodes, {} Workers'.format(bestTPut, txn, key, worker))

    dataFile.close()
    bestDataFile.close()
    print()
    print('FINISHED EXPERIMENT!!!')

def init(txn, key, worker, anna_ip):
    return tools.cluster_init(txn, key, worker, anna_ip)

def warmup(config):
    num_clients = 1
    benchmark = DEFAULT_LAMBDA
    elb = config['elb']
    num_txns = 50
    num_reads = config['reads']
    num_writes = config['writes']
    n_size = config['N']
    cmd = 'python3 benchmark_trigger.py -c {} -l {} -a {} -t {} -r {} -w {} -n {}'
    fmt_cmd = cmd.format(num_clients, benchmark, elb, num_txns, num_reads, num_writes, n_size)
    run_cmd(fmt_cmd)

def clear(anna_ip):
    print('Sleeping and clearing...')
    time.sleep(15)
    tools.clear(True, anna_ip)

def run(config, anna_ip, base_clients=BASE_CLIENTS):
    num_clients = base_clients
    benchmark = DEFAULT_LAMBDA
    elb = config['elb']
    num_txns = int(config['txns'])
    num_reads = int(config['reads'])
    num_writes = int(config['writes'])
    n_size = int(config['N'])

    cmd = 'python3 benchmark_trigger.py -c {} -l {} -a {} -t {} -r {} -w {} -n {}'

    throughputs = []
    print("Beginning...")
    while True:
        print('Trying %d clients...' % num_clients)
        fmt_cmd = cmd.format(num_clients, benchmark, elb, num_txns, num_reads, num_writes, n_size)
        throughput = run_cmd(fmt_cmd)
        print('Got throughput %f' % throughput)

        throughputs.append((num_clients, throughput))

        clear(anna_ip)

        if len(throughputs) > 1 and throughput < throughputs[-2][1]:
            # Decrement by 10 and retry
            num_clients -= 10
            print('Trying %d clients...' % num_clients)
            fmt_cmd = cmd.format(num_clients, benchmark, elb, num_txns, num_reads, num_writes, n_size)
            throughput = run_cmd(fmt_cmd)
            print('Got throughput %f' % throughput)

            throughputs.append((num_clients, throughput))
            clear(anna_ip)

            if throughput > throughputs[-3][1]:
                # Increment by 5 and retry
                num_clients += 5
            else:
                # Decrement by 5
                num_clients -= 5

            print('Trying %d clients...' % num_clients)
            fmt_cmd = cmd.format(num_clients, benchmark, elb, num_txns, num_reads, num_writes, n_size)
            throughput = run_cmd(fmt_cmd)
            print('Got throughput %f' % throughput)
            throughputs.append((num_clients, throughput))

            clear(anna_ip)

            print()
            return throughputs
        else:
            num_clients += 20

def run_cmd(cmd):
    result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    for line in result.stdout:
        line = line.decode("utf-8")
        if "throughput" in line:
            return float(line.split(':')[1])
    print('Throughput not found...exiting')
    os.exit(1)



if __name__ == '__main__':
    main()