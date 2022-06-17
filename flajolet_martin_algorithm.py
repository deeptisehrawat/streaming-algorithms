import binascii
import csv
import sys
import time
from random import sample

from blackbox import BlackBox

NUM_HASH_FUNCS = 50  # optimal :
MAX_HASH_VALUE = 600
GROUP_SIZE = 5
NUM_GROUPS = NUM_HASH_FUNCS / GROUP_SIZE


def generate_hash_params():
    """
    generate hash parameters to be used when calculating hash
    :return: hash parameters (a, b) {'a': list[NUM_HASH_FUNCS], 'b': list[NUM_HASH_FUNCS]}
    """
    hash_params = dict()
    hash_params['a'] = sample(population=range(1, MAX_HASH_VALUE), k=NUM_HASH_FUNCS)
    hash_params['b'] = sample(population=range(1, MAX_HASH_VALUE), k=NUM_HASH_FUNCS)
    return hash_params


def myhashs(user_id, hash_params):
    """
    encapsulates hash functions into a function
    each hash function: f(x)= (ax + b) % m or f(x) = ((ax + b) % p) % m
    :param hash_params:
    :param user_id: String
    :return: list of hash values, for each hash function (size = NUM_HASH_FUNCS)
    """
    hash_values = []
    # m = FILTER_SIZE
    # As the user_id is a string, you need to convert it into an integer and then apply hash functions to it
    x = int(binascii.hexlify(user_id.encode('utf8')), 16)

    for i in range(NUM_HASH_FUNCS):
        hash_value = (hash_params['a'][i] * x + hash_params['b'][i]) % MAX_HASH_VALUE
        # binary_hash_value = bin(hash_value).replace("0b", "")
        hash_values.append(hash_value)
    return hash_values


def flajolet_martin(stream_users, hash_params):
    """
     to estimate the number of unique users within a window in the data stream
    :param stream_users:
    :param hash_params:
    :return: estimation
    """
    unique_users = set()
    max_trailing_zeroes = [0] * NUM_HASH_FUNCS

    for user_id in stream_users:
        unique_users.add(user_id)
        hash_values = myhashs(user_id, hash_params)
        binary_hash_values = [bin(hash_value).replace("0b", "") for hash_value in hash_values]
        trailing_zeroes = [len(str(hash_value)) - len(str(hash_value).rstrip('0')) for hash_value in binary_hash_values]
        max_trailing_zeroes = [max(l1, l2) for l1, l2 in zip(trailing_zeroes, max_trailing_zeroes)]

    estimated_users = [(2 ** r) for r in max_trailing_zeroes]

    # combine estimates
    group_averages = [sum(estimated_users[i:i + GROUP_SIZE]) / GROUP_SIZE for i in range(0, NUM_HASH_FUNCS, GROUP_SIZE)]
    group_averages.sort()
    median_estimate = group_averages[int(NUM_GROUPS / 2)]

    return len(unique_users), median_estimate


def execute_task2():
    if len(sys.argv) > 3:
        input_filename = sys.argv[1]
        stream_size = int(sys.argv[2])
        num_of_asks = int(sys.argv[3])
        output_filename = sys.argv[4]
    else:
        input_filename = './data/users.txt'
        stream_size = 300  # 300
        num_of_asks = 30  # 30
        output_filename = './output/output_task2.csv'

    hash_params = generate_hash_params()
    # print('hash_params[a]: ', hash_params['a'])
    # print('hash_params[b]: ', hash_params['b'])

    bx = BlackBox()
    sum_ground_truth = 0
    sum_estimation = 0
    with open(output_filename, "w+", newline='') as output_file:
        writer = csv.writer(output_file)
        writer.writerow(['Time', 'Ground Truth', 'Estimation'])

        for batch_idx in range(num_of_asks):
            stream_users = bx.ask(input_filename, stream_size)
            ground_truth, estimation = flajolet_martin(stream_users, hash_params)
            writer.writerow([batch_idx, ground_truth, int(estimation)])
            sum_ground_truth += ground_truth
            sum_estimation += estimation

    # should be between 0.2 and 5
    print('Error: ', sum_estimation / sum_ground_truth)


if __name__ == '__main__':
    start_time = time.time()
    execute_task2()
    print('Execution time: ', time.time() - start_time)
