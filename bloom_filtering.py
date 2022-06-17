import binascii
import csv
import sys
import time
from random import sample

from blackbox import BlackBox

NUM_HASH_FUNCS = 20  # optimal : (FILTER_SIZE / maxlen(members)) * ln(2)
FILTER_SIZE = 69997
bloom_filter = [0] * FILTER_SIZE
members = set()


def generate_hash_params():
    """
    generate hash parameters to be used when calculating hash
    :return: hash parameters (a, b) {'a': list[NUM_HASH_FUNCS], 'b': list[NUM_HASH_FUNCS]}
    """
    hash_params = dict()
    hash_params['a'] = sample(population=range(1, FILTER_SIZE), k=NUM_HASH_FUNCS)
    hash_params['b'] = sample(population=range(1, FILTER_SIZE), k=NUM_HASH_FUNCS)
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
    # print("x: ", x)

    for i in range(NUM_HASH_FUNCS):
        hash_value = (hash_params['a'][i] * x + hash_params['b'][i]) % FILTER_SIZE
        hash_values.append(hash_value)
    return hash_values


def bloom_filtering(stream_users, hash_params):
    global bloom_filter, members
    false_positive = 0
    true_negative = 0

    for user_id in stream_users:
        if user_id in members:
            continue
        members.add(user_id)

        hash_values = myhashs(user_id, hash_params)
        # 1. application
        bits_already_set = 0
        for i in range(NUM_HASH_FUNCS):
            if bloom_filter[hash_values[i]] == 1:
                bits_already_set += 1

        if bits_already_set == NUM_HASH_FUNCS:  # false positive
            false_positive += 1
        else:
            true_negative += 1
            # 2. construction
            for i in range(NUM_HASH_FUNCS):
                bloom_filter[hash_values[i]] = 1

    false_positive_rate = false_positive / (false_positive + true_negative)
    return false_positive_rate


def execute_task1():
    if len(sys.argv) > 3:
        input_filename = sys.argv[1]
        stream_size = int(sys.argv[2])
        num_of_asks = int(sys.argv[3])
        output_filename = sys.argv[4]
    else:
        input_filename = './data/users.txt'
        stream_size = 100   # 100
        num_of_asks = 30   # 30
        output_filename = './output/output_task1.csv'

    hash_params = generate_hash_params()
    # print('hash_params[a]: ', hash_params['a'])
    # print('hash_params[b]: ', hash_params['b'])

    bx = BlackBox()
    with open(output_filename, "w+", newline='') as output_file:
        writer = csv.writer(output_file)
        writer.writerow(['Time', 'FPR'])

        for batch_idx in range(num_of_asks):
            # user ids might not be unique
            stream_users = bx.ask(input_filename, stream_size)
            false_positive_rate = bloom_filtering(stream_users, hash_params)
            writer.writerow([batch_idx, false_positive_rate])


if __name__ == '__main__':
    start_time = time.time()
    execute_task1()
    print('Execution time: ', time.time() - start_time)
