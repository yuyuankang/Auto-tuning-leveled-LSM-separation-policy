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

    tlsm_write_amplification_list = tlsm.history_write_amplification_rate[
                                    len(tlsm.history_write_amplification_rate) - statistics_number:]
    if (len(tlsm_write_amplification_list) == 0):
        tlsm_write_amplification_rate = 0
    else:
        tlsm_write_amplification_rate = np.average(tlsm.history_write_amplification_rate[
                                                   len(tlsm.history_write_amplification_rate) - statistics_number:])

    # collect statistic result
    # how many pts to rewrite in each cycle
    history_rewrite_sstable_and_point_number = tlsm.history_rewrite_sstable_and_point_number
    try:
        rewrite_data_point_number = list(np.array(history_rewrite_sstable_and_point_number)[:, 4])
    except IndexError:
        average_rewrite_data_point_number = 0
    else:
        average_rewrite_data_point_number = np.average(
            rewrite_data_point_number[len(rewrite_data_point_number) - statistics_number:])
    try:
        merge_sorted_files_number = list(np.array(history_rewrite_sstable_and_point_number)[:, 0])
    except IndexError:
        average_merge_sorted_files_number = 0
    else:
        average_merge_sorted_files_number = np.average(
            merge_sorted_files_number[len(merge_sorted_files_number) - statistics_number:])

    try:
        direct_flushed_files_number = list(np.array(history_rewrite_sstable_and_point_number)[:, 1])
    except IndexError:
        average_direct_flushed_files_number = 0
    else:
        average_direct_flushed_files_number = np.average(
            direct_flushed_files_number[len(direct_flushed_files_number) - statistics_number:])

    try:
        merge_sorted_points_number = list(np.array(history_rewrite_sstable_and_point_number)[:, 2])
    except IndexError:
        average_merge_sorted_points_number = 0
    else:
        average_merge_sorted_points_number = np.average(
            merge_sorted_points_number[len(merge_sorted_points_number) - statistics_number:])

    try:
        direct_flushed_points_number = list(np.array(history_rewrite_sstable_and_point_number)[:, 3])
    except IndexError:
        average_direct_flushed_points_number = 0
    else:
        average_direct_flushed_points_number = np.average(
            direct_flushed_points_number[len(direct_flushed_points_number) - statistics_number:])
    # data point arrives in a cycle
    points_number_in_a_cycle = tlsm.history_points_number_in_a_cycle
    average_points_number_in_a_cycle = np.average(
        points_number_in_a_cycle[len(points_number_in_a_cycle) - statistics_number:])

    # nonsequential_point_number_when_sequential_buffer_if_full
    g_function = tlsm.history_nonsequential_point_number_when_sequential_buffer_if_full
    average_g_function = np.average(g_function[
                                    len(g_function) - statistics_number:])

    # eval_old_cycle_merge = average_merge_sorted_files_number * (
    #         sequential_buffer_size + nonsequential_buffer_size) + sequential_buffer_size
    # eval_cur_cycle_merge = sequential_buffer_size * float(nonsequential_buffer_size / average_g_function) - sequential_buffer_size
    # eval_new_unseq = nonsequential_buffer_size
    # eval_new_seq = sequential_buffer_size * float(nonsequential_buffer_size / average_g_function)
    # eval_speed = float(eval_old_cycle_merge + eval_cur_cycle_merge) / (eval_new_unseq + eval_new_seq)

    total_data_num, total_write_num = tlsm.get_write_amplification()

    return tlsm_write_amplification_rate, \
           average_rewrite_data_point_number, \
           average_merge_sorted_files_number, \
           average_direct_flushed_files_number, \
           average_merge_sorted_points_number, \
           average_direct_flushed_points_number, \
           average_points_number_in_a_cycle, \
           average_g_function, total_data_num, total_write_num


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


def works(process_id, settings, total_num, buffer_size, statistics_number,
          seq_size_inc_rate, print_lock):
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
    mus = [4, 4.5, 5]
    sigmas = [0.5, 1, 1.5, 2, 2.5]

    for setting in settings:
        mu, sigma, time_interval = setting

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

        # with print_lock:
        #     print('process_id=' + str(process_id),
        #           'mu=' + str(mu),
        #           'sigma=' + str(sigma),
        #           'time_interval=' + str(time_interval),
        #           'lsm_average_sstable_merge_number=' + str(lsm_average_sstable_merge_number))
        #     sys.stdout.flush()

        # write tLSM structure
        for sequential_buffer_size in range(1, int(0.9 * buffer_size), seq_size_inc_rate):
            nonsequential_buffer_size = buffer_size - sequential_buffer_size
            tlsm_write_amplification_rate, \
            average_rewrite_data_point_number, \
            average_merge_sorted_files_number, \
            average_direct_flushed_files_number, \
            average_merge_sorted_points_number, \
            average_direct_flushed_points_number, \
            average_points_number_in_a_cycle, \
            average_g_function, total_data_num, total_write_num = experiment(sequential_buffer_size,
                                                                             nonsequential_buffer_size,
                                                                             data_points,
                                                                             statistics_number)

            ratio = float(lsm_average_sstable_merge_number / tlsm_write_amplification_rate)
            with print_lock:
                print('process_id=' + str(process_id),
                      'mu=' + str(mu),
                      'sigma=' + str(sigma),
                      'time_interval=' + str(time_interval),
                      'sequential_buffer_size=' + str(sequential_buffer_size),
                      'lsm_average_sstable_merge_number=' + str(lsm_average_sstable_merge_number),
                      'tlsm_write_amplification_rate=' + str(tlsm_write_amplification_rate),
                      'rlsm/rtlsm=' + str(ratio),
                      'average_rewrite_data_point_number=' + str(average_rewrite_data_point_number),
                      'average_merge_sorted_files_number=' + str(average_merge_sorted_files_number),
                      'average_direct_flushed_files_number=' + str(average_direct_flushed_files_number),
                      'average_merge_sorted_points_number=' + str(average_merge_sorted_points_number),
                      'average_direct_flushed_points_number=' + str(average_direct_flushed_points_number),
                      'average_points_number_in_a_cycle=' + str(average_points_number_in_a_cycle),
                      'average_g_function=' + str(average_g_function),
                      'total_data_num=' + str(total_data_num),
                      'total_write_num=' + str(total_write_num)
                      )
                sys.stdout.flush()


if __name__ == '__main__':

    possible_intervals = range_float(arg_min_interval, arg_max_interval, arg_interval_step)

    possible_settings = [[3.0, 0.5, 7.5],
                         [3.0, 0.5, 7.625],
                         [3.0, 0.5, 7.75],
                         [3.0, 0.5, 7.875],
                         [3.0, 0.5, 8.0],
                         [3.0, 0.5, 8.125],
                         [3.0, 0.5, 8.25],
                         [3.0, 0.5, 8.375],
                         [3.0, 0.5, 8.5],
                         [3.0, 0.5, 8.625],
                         [3.0, 0.5, 8.75],
                         [3.0, 0.5, 8.875],
                         [3.0, 0.5, 9.0],
                         [3.0, 0.5, 9.125],
                         [3.0, 0.5, 9.25],
                         [3.0, 0.5, 9.375],
                         [3.0, 0.5, 9.5],
                         [3.0, 0.5, 9.625],
                         [3.0, 0.5, 9.75],
                         [3.0, 0.5, 9.875],
                         [3.0, 0.5, 10.0],
                         [3.0, 0.5, 10.125],
                         [3.0, 0.5, 10.25],
                         [3.0, 0.5, 10.375],
                         [3.0, 0.5, 10.5],
                         [3.0, 0.5, 10.625],
                         [3.0, 0.5, 10.75],
                         [3.0, 0.5, 10.875],
                         [3.0, 0.5, 11.0],
                         [3.0, 0.5, 11.125],
                         [3.0, 0.5, 11.25],
                         [3.0, 0.5, 11.375],
                         [3.0, 0.5, 11.5],
                         [3.0, 0.5, 11.625],
                         [3.0, 0.5, 11.75],
                         [3.0, 0.5, 11.875],
                         [3.0, 0.5, 12.0],
                         [3.0, 0.5, 12.125],
                         [3.0, 0.5, 12.25],
                         [3.0, 0.5, 12.375],
                         [3.0, 0.5, 12.5],
                         [3.0, 0.5, 12.625],
                         [3.0, 0.5, 12.75],
                         [3.0, 0.5, 12.875],
                         [3.0, 0.5, 13.0],
                         [3.0, 0.5, 13.125],
                         [3.0, 0.5, 13.25],
                         [3.0, 0.5, 13.375],
                         [3.0, 0.5, 13.5],
                         [3.0, 0.5, 13.625],
                         [3.0, 0.5, 13.75],
                         [3.0, 0.5, 13.875],
                         [3.0, 0.5, 14.0],
                         [3.0, 0.5, 14.125],
                         [3.0, 0.5, 14.25],
                         [3.0, 0.5, 14.375],
                         [3.0, 0.5, 14.5],
                         [3.0, 0.5, 14.625],
                         [3.0, 0.5, 14.75],
                         [3.0, 0.5, 14.875],
                         [3.0, 0.5, 15.0],
                         [3.0, 0.5, 15.125],
                         [3.0, 0.5, 15.25],
                         [3.0, 0.5, 15.375],
                         [3.0, 0.5, 15.5],
                         [3.0, 0.5, 15.625],
                         [3.0, 0.5, 15.75],
                         [3.0, 0.5, 15.875],
                         [3.0, 0.5, 16.0],
                         [3.0, 0.5, 16.125],
                         [3.0, 0.5, 16.25],
                         [3.0, 0.5, 16.375],
                         [3.0, 0.5, 16.5],
                         [3.0, 0.5, 16.625],
                         [3.0, 0.5, 16.75],
                         [3.0, 0.5, 16.875],
                         [3.0, 0.5, 17.0],
                         [3.0, 0.5, 17.125],
                         [3.0, 0.5, 17.25],
                         [3.0, 0.5, 17.375],
                         [3.0, 0.5, 17.5],
                         [3.0, 0.5, 17.625],
                         [3.0, 0.5, 17.75],
                         [3.0, 0.5, 17.875],
                         [3.0, 0.5, 18.0],
                         [3.0, 0.5, 18.125],
                         [3.0, 0.5, 18.25],
                         [3.0, 0.5, 18.375],
                         [3.0, 0.5, 18.5],
                         [3.0, 0.5, 18.625],
                         [3.0, 0.5, 18.75],
                         [3.0, 0.5, 18.875],
                         [3.0, 0.5, 19.0],
                         [3.0, 0.5, 19.125],
                         [3.0, 0.5, 19.25],
                         [3.0, 0.5, 19.375],
                         [3.0, 0.5, 19.5],
                         [3.0, 0.5, 19.625],
                         [3.0, 0.5, 19.75],
                         [3.0, 0.5, 19.875],
                         [3.0, 0.5, 20.0]
                         ]

    np.random.shuffle(possible_settings)

    settings = distribute_tasks(possible_settings, process_num)
    lock = multiprocessing.Lock()
    processes = []

    # delays = Delay(2048).get_delays(arg_total_num)
    for i in range(process_num):
        process = multiprocessing.Process(target=works,
                                          args=(i, settings[i], arg_total_num, arg_buffer_size,
                                                arg_statistic_number, arg_sequential_buffer_increase_step,
                                                lock))
        process.start()
        processes.append(process)
    for process in processes:
        process.join()
