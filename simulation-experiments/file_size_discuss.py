import multiprocessing
import sys

from algorithm_utils import *
from lsm import LSM
from tlsm import tLSM

np.random.seed(4834)

arg_min_interval = 0.05
arg_max_interval = 20
arg_interval_step = 0.05
arg_total_num = 100000
arg_buffer_size = 512
arg_statistic_number = 80
arg_mu = 4
arg_sigma = 1.5
arg_sequential_buffer_increase_step = 5
process_num = 191


def experiment(sequential_buffer_size, nonsequential_buffer_size, points, statistics_number):
    """
    Conduct an experiment on tlsm, write the data points to a tlsm structure
    :param sequential_buffer_size: capacity of the sequential buffer
    :param nonsequential_buffer_size: capacity of the non-sequential buffer
    :param points: data points, a list of generate time
    :param statistics_number: the number of observations in calculating statistics
    :return: statistic results
    """
    tlsm = tLSM(sequential_buffer_size, nonsequential_buffer_size,
                sequential_buffer_size + nonsequential_buffer_size)

    for val in points:
        tlsm.write(val)

    normal_size_counter = 0
    small_size_counter = 0
    total_size = 0
    for sst in tlsm.level_1:
        sstable_size = len(sst.data_list)
        total_size += sstable_size
        if sstable_size == sequential_buffer_size + nonsequential_buffer_size:
            normal_size_counter += 1
        else:
            small_size_counter += 1

    average_size = total_size / (normal_size_counter + small_size_counter)

    return average_size, normal_size_counter, small_size_counter, float(normal_size_counter) / (
            normal_size_counter + small_size_counter)


def distribute_tasks(tasks, number):
    """
    Distribute the tasks into multiple groups
    :param tasks: a list of tasks
    :param number: number of groups
    :return: a list of task groups
    """
    task_groups = []
    for i in range(number):
        task_groups.append([])
    i = 0
    for element in tasks:
        task_groups[i % number].append(element)
        i += 1
    return task_groups


def works(process_id, time_intervals, total_num, buffer_size, statistics_number,
          seq_size_inc_rate, mu, sigma, print_lock):
    """
    A worker corresponds to a process, which executes several tasks
    :param process_id: id of the process
    :param time_intervals: generate time
    :param total_num: total number of data points to generate
    :param buffer_size: capacity of buffer for lsm
    :param statistics_number: the number of observations in calculating statistics
    :param seq_size_inc_rate: the step of increase the capacity setting of sequential buffer
    :param mu: parameter of lognormal distribution, mu
    :param sigma: parameter of lognormal distribution, sigma
    :param print_lock: process lock, for printing logs
    """
    counter = 0
    for time_interval in time_intervals:
        # generate data points
        data_points = generate_data_points(time_interval, total_num, mu, sigma)

        # write LSM structure
        lsm = LSM(buffer_size, buffer_size, statistics_number)
        for val in data_points:
            lsm.write(val)
        # make sure the statistic number is valid
        assert len(lsm.history_merge_sstable_number) * 0.6 - statistics_number > 0
        lsm_average_write_amplification_rate = lsm.average_write_amplification_rate()
        lsm_average_sstable_merge_number = lsm_average_write_amplification_rate

        # write tLSM structure
        for sequential_buffer_size in range(1, int(0.9 * buffer_size), seq_size_inc_rate):
            nonsequential_buffer_size = buffer_size - sequential_buffer_size
            average_size, normal_size_counter, small_size_counter, normal_ratio = experiment(sequential_buffer_size,
                                                                                             nonsequential_buffer_size,
                                                                                             data_points,
                                                                                             statistics_number)
            with print_lock:
                print('process_id=' + str(process_id),
                      'time_interval=' + str(time_interval),
                      'sequential_buffer_size=' + str(sequential_buffer_size),
                      'average_size='+str(average_size),
                      'normal_size_counter='+str(normal_size_counter),
                      'small_size_counter='+str(small_size_counter),
                      'normal_ratio='+str(normal_ratio))
                sys.stdout.flush()
        counter += 1


if __name__ == '__main__':

    possible_intervals = range_float(arg_min_interval, arg_max_interval, arg_interval_step)
    np.random.shuffle(possible_intervals)

    intervals = distribute_tasks(possible_intervals, process_num)
    lock = multiprocessing.Lock()
    processes = []

    # delays = Delay(2048).get_delays(arg_total_num)
    for i in range(process_num):
        process = multiprocessing.Process(target=works,
                                          args=(i, intervals[i], arg_total_num, arg_buffer_size,
                                                arg_statistic_number, arg_sequential_buffer_increase_step, arg_mu,
                                                arg_sigma, lock))
        process.start()
        processes.append(process)
    for process in processes:
        process.join()
