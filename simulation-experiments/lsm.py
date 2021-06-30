import numpy as np

from sstable import SSTable
from algorithm_utils import merge_sort

def count_point(ssts):
    ret = 0
    for sst in ssts:
        assert sst.index == 0
        ret += len(sst.data_list)
    return ret

class LSM:
    def __init__(self, buffer_size=8, sstable_size=None, statistics_number=20):
        """
        Initialize a 2-level LSM structure
        :param buffer_size: the capacity of component in memory, that is C0
        :param sstable_size: the capacity of component in L1, which is also called sstable
        :param statistics_number: the number of observations in calculating statistics
        """

        self.buffer_size = buffer_size
        # if sstable size is not set, make the capacity equal to the capacity of sstables
        self.sstable_size = sstable_size if sstable_size is not None else buffer_size
        # the component in memory, C0
        self.buffer = []
        # LEVEL1, which is a list of sstables
        self.level_1 = []

        # record the number of sstables to merge in each cycle
        self.history_merge_sstable_number = []
        # record the write amplification rate evaluated in each cycle
        self.history_write_amplification_rate = []
        self.statistics_number = statistics_number

        self.total_write_times = 0

    def write(self, val):
        """
        Write a value val to the LSM structure
        :param val: the value to write
        """
        # append the value to the buffer C0
        self.buffer.append(val)
        # if the buffer C0 is full, form an sstable and write it to the LEVEL1
        if len(self.buffer) == self.buffer_size:
            self.__write_buffer()

    def __write_buffer(self):
        """
        Form a sstable with whatever inside the buffer C0, write it to L1, and finally clear the buffer
        :return:
        """
        if len(self.buffer) > 0:
            # retrieve the content in buffer
            data_point_list = self.buffer.copy()
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
            self.buffer.clear()

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
        self.history_merge_sstable_number.append(len(merge_list))
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

    def flush(self):
        """
        Flush whatever inside buffer to LEVEL1
        """
        self.__write_buffer()

    def average_write_amplification_rate(self):
        return np.average(self.history_merge_sstable_number[len(self.history_merge_sstable_number) - self.statistics_number:])

    def all_merged_file_num(self):
        return sum(self.history_merge_sstable_number)

    def print_(self):
        print('LSM: ')
        print('LEVEL0:', str(self.buffer))
        print('LEVEL1:', str(self.level_1))
