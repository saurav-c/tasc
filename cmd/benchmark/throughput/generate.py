import sys
import numpy as np
from functools import cmp_to_key

def compare(a, b):
    if a[2] != b[2]:
        return a[2] - b[2]
    return a[1] - b[1] if a[1] != b[1] else a[0] - b[0]

def main():
    args = sys.argv[1:]
    txn, key, worker = int(args[0]), int(args[1]), int(args[2])
    samples = int(args[3])

    data = []
    with open('generated.txt', 'w+') as f:
        for i in range(1, txn+1):
            for j in range(1, key+1):
                for k in range(1, worker+1):
                    data.append((i, j, k))
                    f.write('{},{},{}\n'.format(i, j, k))

    np.random.shuffle(data)
    data = data[:samples]

    data = sorted(data, key=cmp_to_key(compare))

    with open('cluster_config.txt', 'w+') as f:
        for x in data:
            f.write('{},{},{}\n'.format(x[0], x[1], x[2]))

if __name__ == '__main__':
    main()