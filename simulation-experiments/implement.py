import math
import matplotlib.pyplot as plt
import numpy as np
from queue import Queue

from algorithm_utils import merge_sort, generate_data_points_with_delay, generate_data_points_real_delay, \
    generate_data_points_real_delay_with_delay
from lsm import LSM
from sstable import SSTable


def count_point(ssts):
    ret = 0
    for sst in ssts:
        assert sst.index == 0
        ret += len(sst.data_list)
    return ret


class BufferedQueue:
    """
    A buffered queue. The size of the queue is fixed. Elements are constantly added to the queue.
    When the queue is full, the oldest element is removed to make room for the new element.
    """

    def __init__(self, maxsize) -> None:
        """
        Constructor of a BufferedQueue
        :param maxsize: maximal number of element the queue can hold
        """
        super().__init__()
        self.queue = Queue(maxsize=maxsize)
        self.element_sum = 0
        self.element_number = 0

    def put(self, val):
        """
        Add a element to the buffered queue
        :param val: the element
        """
        if self.queue.full():
            self.element_sum -= self.queue.get()
        else:
            self.element_number += 1
        self.queue.put(val)
        self.element_sum += val

    def full(self):
        return self.queue.full()

    def average(self):
        """
        Expected value of whatever in the buffered queue
        :return: the expected value
        """
        return float(self.element_sum) / self.element_number


class DelayStatistics:
    """
    Statistics on delays
    """

    def __init__(self, delays, max_delay=None, bucket_num=None) -> None:
        """
        Describes the distribution of the observed delay list
        :param delays: the delay list
        :param max_delay: maximal delay, which is related to the x axis of discrete CDF function
        :param bucket_num: bucket number of the discrete CDF function
        """
        super().__init__()
        self.delays = delays
        self.max_delay = max_delay if max_delay is not None else max(delays)
        self.bucket_num = bucket_num if bucket_num is not None else math.ceil(float(len(self.delays)) / 10)
        self.f_value, self.x_bins, _ \
            = plt.hist(self.delays, self.bucket_num, range=[0, self.max_delay], density=1, cumulative=True)
        self.step = self.x_bins[1]

    def F(self, val):
        """
        The discrete CDF function.
        :param val: input delay
        :return: the probability of delay being less than the input value
        """
        return 1 if val >= self.max_delay else self.f_value[int(val / self.step)]


class Hybrid:

    def __init__(self, lsm_buffer_size, generate_time_interval, sstable_size=None, delay_buffer_size=2000,
                 statistics_number=20, min_sequential_buffer_size=128, print_all_n1=False) -> None:
        super().__init__()
        self.print_all_n1 = print_all_n1
        self.total_write_times = 0
        self.generate_time_interval = generate_time_interval

        # LSM structure
        self.lsm_buffer_size = lsm_buffer_size
        self.lsm_buffer = []

        # tLSM structure, sequential buffer and nonsequential buffer
        self.sequential_buffer = []
        self.nonsequential_buffer = []
        self.sequential_buffer_size = None
        self.nonsequential_buffer_size = None
        self.max_generate_time_on_level_1 = 0

        # LEVEL1, for storing sstables
        self.level_1 = []
        self.sstable_size = self.lsm_buffer_size if sstable_size is None else sstable_size

        # strategy
        self.use_tlsm = False

        # statistics
        self.delay_buffer_size = delay_buffer_size
        self.lsm_eta_list = BufferedQueue(maxsize=statistics_number)
        # self.tlsm_eta_list = BufferedQueue(maxsize=statistics_number)
        self.delays = []

        self.statistics_number = statistics_number

        self.min_sequential_buffer_size = min_sequential_buffer_size

        self.history_write_amplification_rate = []

    def __to_use_tlsm(self):
        return len(self.delays) >= self.statistics_number and self.lsm_eta_list.full()

    def __get_candidate_n1(self):
        delay_analysis = DelayStatistics(self.delays)
        sum_list = [-1]
        for i in range(1, self.lsm_buffer_size):
            last = 0 if i == 1 else sum_list[len(sum_list) - 1]
            sum_list.append(last + delay_analysis.F(i * self.generate_time_interval))

        g_value_list = []
        n1_list = []

        expected_eta = self.lsm_eta_list.average()

        for g_plus_n1 in range(self.min_sequential_buffer_size, self.lsm_buffer_size):
            g_value = g_plus_n1 - sum_list[g_plus_n1]
            n1_value = g_plus_n1 - g_value
            g_value_list.append(g_value)
            n1_list.append(n1_value)

        print(n1_list)
        print(g_value_list)
        g_value_list = np.array(g_value_list)
        n1_list = np.array(n1_list)
        n2_list = self.lsm_buffer_size - n1_list
        # print(n1_list)
        tmp = n1_list * n2_list / g_value_list
        # print('temp', tmp)
        r_tlsm = 2 + (expected_eta * self.lsm_buffer_size) / (tmp + n2_list)

        if self.print_all_n1:
            print(','.join(n1_list))
            print(','.join(r_tlsm))
        # print('rtlsm', r_tlsm)
        min_rate = np.min(r_tlsm)
        min_rate_index = np.argmin(r_tlsm)
        return np.round(n1_list[min_rate_index])

    def __write_lsm_buffer(self):
        if len(self.lsm_buffer) > 0:
            # retrieve the content in buffer
            data_point_list = self.lsm_buffer.copy()
            # sort the data points by generate time
            data_point_list.sort()
            # a list of <generate_time, write_times> pairs
            pairs = []
            for i in data_point_list:
                # for each data points, form a pair <generate_time, write_times>, write_times is initialize as 0
                pairs.append([i, 0])
            # form an sstable
            sst = SSTable(pairs, data_point_list[0], data_point_list[len(data_point_list) - 1], False)
            self.__merge(sst)
            self.lsm_buffer.clear()

    def __merge(self, new_sstable):
        """
        Merge a new sstable to LEVEL1
        :param new_sstable: a new sstable
        """
        # record the sstables need to participate in merge sort
        merge_list = []
        # iterate the sstables in LEVEL1 from tail (with later generate time) to head (with earlier generate time)
        # to check whether it has overlapped generate time range with the new sstable
        while len(self.level_1) > 0:
            last_sstable = self.level_1.pop()
            if last_sstable.max_val > new_sstable.min_val:
                # overlapped generate time range exists, remove it from LEVEL1 and append the sstable to the list
                merge_list.append(last_sstable)
            else:
                # current sstable has no overlap generate time range with the new sstable
                # put it back to the tail of LEVEL1
                self.level_1.append(last_sstable)
                break
        # record the number of sstables in LEVEL1 to merge
        self.lsm_eta_list.put(len(merge_list))
        # record the write amplification history
        self.history_write_amplification_rate.append(len(merge_list))

        # if there's no sstables on LEVEL1 to be merged, append the new sstable to the tail of LEVEL1 directly
        if len(merge_list) == 0:
            # Because it simulate that the data points is written to the disk, we increase the write times of
            # each data points by one
            new_sstable.rewrite()
            self.total_write_times += len(new_sstable.data_list)
            self.level_1.append(new_sstable)
        else:
            # append the new sstable to the merge list, apply merge sort generate several sstables, and append
            # the at the end of LEVEL1
            merge_list.append(new_sstable)
            self.total_write_times += count_point(merge_list)
            self.level_1.extend(merge_sort(merge_list, self.sstable_size))

    def __write_lsm(self, val):
        # append the value to the buffer C0
        self.lsm_buffer.append(val)
        # if the buffer C0 is full, form an sstable and write it to the LEVEL1
        if len(self.lsm_buffer) == self.lsm_buffer_size:
            self.__write_lsm_buffer()

    def __is_sequential(self, val):
        """
        Check whether a data point is sequential or not
        :param val: the generate time of a data point
        :return: is sequential or not
        """
        return val > self.max_generate_time_on_level_1

    def __write_sequential_buffer(self):
        """
        Write the content inside sequential buffer to LEVEL1, and finally clear the sequential buffer
        """
        if len(self.sequential_buffer) > 0:
            data_point_list = self.sequential_buffer.copy()
            data_point_list.sort()
            pairs = []
            for i in data_point_list:
                # Because the sstable is directly write to LEVEL1, the write number of each data point is initialized
                # as 1
                pairs.append([i, 1])
            sstable = SSTable(pairs, data_point_list[0], data_point_list[len(data_point_list) - 1], 0, False)
            self.max_generate_time_on_level_1 = data_point_list[len(data_point_list) - 1]
            # append the new sstable to the tail of LEVEL1 directly, without merge
            self.total_write_times += len(sstable.data_list)
            self.level_1.append(sstable)
            self.sequential_buffer.clear()

    def __write_nonsequential_buffer(self):
        """
        Write the content inside nonsequential buffer to LEVEL1, and finally clear the nonsequential buffer
        """
        if len(self.nonsequential_buffer) > 0:
            # retrieve the content inside nonsequential buffer
            data_point_list = self.nonsequential_buffer.copy()
            data_point_list.sort()
            pairs = []
            for i in data_point_list:
                pairs.append([i, 0])
            # form a new sstable
            sstable = SSTable(pairs, data_point_list[0], data_point_list[len(data_point_list) - 1], 0)
            # merge the new sstable to LEVEL1
            self.__merge(sstable)
            self.nonsequential_buffer.clear()

    def __write_tlsm(self, val):
        if self.print_all_n1:
            exit(0)
        if self.__is_sequential(val):
            self.sequential_buffer.append(val)
            if len(self.sequential_buffer) == self.sequential_buffer_size:
                self.__write_sequential_buffer()
        else:
            self.nonsequential_buffer.append(val)
            if len(self.nonsequential_buffer) == self.nonsequential_buffer_size:
                self.__write_nonsequential_buffer()

    def __set_sequential_buffer_size(self, size):
        self.sequential_buffer_size = size
        self.nonsequential_buffer_size = self.lsm_buffer_size - size

    def write(self, val, delay):
        if self.use_tlsm is False:
            if self.__to_use_tlsm():
                self.__write_lsm_buffer()
                self.__set_sequential_buffer_size(self.__get_candidate_n1())
                self.max_generate_time_on_level_1 = self.level_1[len(self.level_1) - 1].max_val
                self.use_tlsm = True
                print('use tlsm, seq buffer=', self.sequential_buffer_size)
            else:
                self.delays.append(delay)
                self.use_tlsm = False
        self.__write_lsm(val) if not self.use_tlsm else self.__write_tlsm(val)


if __name__ == '__main__':
    arg_time_interval = 50
    # arg_data_point_number = 10000000
    arg_data_point_number = 10000000
    arg_buffer_size = 5120
    arg_statistics_num = 200
    # data_points = generate_data_points_with_delay(arg_time_interval, arg_data_point_number, mu=4, sigma=1.5)
    data_points = generate_data_points_real_delay_with_delay(arg_time_interval, arg_data_point_number, 'ty.txt')

    lsm = LSM(buffer_size=arg_buffer_size)
    hybrid = Hybrid(lsm_buffer_size=arg_buffer_size, generate_time_interval=arg_time_interval, statistics_number=arg_statistics_num, print_all_n1=True)

    counter = 0
    s11_ = 0
    s22_ = 0
    for point in data_points:
        hybrid.write(point[0], point[2])
        lsm.write(point[0])
        counter += 1

        if counter % arg_buffer_size == 0:
            # s1 = 0
            # for sst in hybrid.level_1:
            #     s1 += sst.get_write_times()
            s11 = hybrid.total_write_times

            collected_delay = len(hybrid.delays)
            collected_eta = hybrid.lsm_eta_list.element_number

            # s2 = 0
            # for sst in lsm.level_1:
            #     s2 += sst.get_write_times()
            s22 = lsm.total_write_times
            print(counter, s11, s22, s11 - s11_, s22 - s22_, collected_delay/arg_statistics_num, collected_eta/arg_statistics_num)
            s11_ = s11
            s22_ = s22
