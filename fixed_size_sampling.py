import csv
import random
import sys
import time

from blackbox import BlackBox

FIXED_SIZE = 100    # fixed size sampling, size = 100
users_sample = []   # sampling of users
num_users = 0   # num of users till now


def reservoir_sampling(stream_users):
    """
    fixed size sampling
    :param stream_users:
    :return:
    """
    global users_sample
    global num_users

    for user_id in stream_users:
        num_users += 1
        if len(users_sample) < FIXED_SIZE:
            users_sample.append(user_id)
            continue

        # accept or discard the sample
        random_prob = random.random()
        if random_prob < FIXED_SIZE / num_users:
            replacement_idx = random.randint(0, FIXED_SIZE-1)
            users_sample[replacement_idx] = user_id


def execute_task3():
    if len(sys.argv) > 3:
        input_filename = sys.argv[1]
        stream_size = int(sys.argv[2])
        num_of_asks = int(sys.argv[3])
        output_filename = sys.argv[4]
    else:
        input_filename = './data/users.txt'
        stream_size = 100   # 100
        num_of_asks = 100    # 30
        output_filename = './output/output_task3.csv'

    bx = BlackBox()
    with open(output_filename, "w+", newline='') as output_file:
        writer = csv.writer(output_file)
        writer.writerow(['seqnum', '0_id', '20_id', '40_id', '60_id', '80_id'])

        for batch_idx in range(num_of_asks):
            stream_users = bx.ask(input_filename, stream_size)
            reservoir_sampling(stream_users)

            if num_users != 0 and num_users % 100 == 0:
                writer.writerow([num_users, users_sample[0], users_sample[20], users_sample[40], users_sample[60],
                                 users_sample[80]])


if __name__ == '__main__':
    start_time = time.time()
    random.seed(553)    # random number generator needs a num to start with (a seed value)
    execute_task3()
    print('Execution time: ', time.time() - start_time)
