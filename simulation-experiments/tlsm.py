from sstable import SSTable
from algorithm_utils import merge_sort


def count(sstables):
    """
    We categorize the sstables in 2 types.
    Type1: the sstable is generated from merge sort
    Type2: the sstable is formed by sequential buffer directly
    This method counts the number of sstables of both types, and their corresponding data points number in the
    given sstable list
    :param sstables: a list of sstables
    """
    merge_sorted_sstable_number = 0
    direct_flushed_sstable_number = 0
    merge_sorted_points_number = 0
    direct_flushed_points_number = 0
    for sst in sstables:
        if sst.is_from_merge_sort:
            merge_sorted_sstable_number += 1
            merge_sorted_points_number += len(sst.data_list)
        else:
            direct_flushed_sstable_number += 1
            direct_flushed_points_number += len(sst.data_list)
    return merge_sorted_sstable_number, direct_flushed_sstable_number, merge_sorted_points_number, \
           direct_flushed_points_number


def count_point(ssts):
    ret = 0
    for sst in ssts:
        assert sst.index == 0
        ret += len(sst.data_list)
    return ret


class tLSM:
    def __init__(self, sequential_buffer_size=8, nonsequential_buffer_size=8, sstable_size=None):
        """
        Initialize a 2-level tLSM structure, which consists of 2 component in memory, and LEVEL1 for storing sstables
        :param sequential_buffer_size: capacity of sequential buffer in memory
        :param nonsequential_buffer_size: capacity of non-sequential buffer in memory
        :param sstable_size: capacity of components/sstables on LEVEL1
        """
        # if the capacity of output sstable is not set, use the sum of the sequential buffer size and non-sequential 
        # buffer size
        self.write_times = 0
        self.sstable_size = sstable_size if sstable_size is not None \
            else sequential_buffer_size + nonsequential_buffer_size

        # initialize sequential buffer and nonsequential buffer
        self.sequential_buffer = []
        self.nonsequential_buffer = []
        self.sequential_buffer_size = sequential_buffer_size
        self.nonsequential_buffer_size = nonsequential_buffer_size

        # initialize LEVEL1
        self.sstable_size = sstable_size
        self.level_1 = []
        self.max_generate_time_on_level_1 = 0

        # record how many sequence files are generated during a cycle
        self.sequential_buffer_flush_times_per_cycle = 0
        self.history_sequential_buffer_flush_times_per_cycle = []

        # how many unseq pts arrives during flushing sequence file
        self.history_nonsequential_point_number_when_sequential_buffer_if_full = []
        self.nonsequential_point_number_when_sequential_buffer_is_full = 0

        # how many data points (sequential and nonsequential) arrive during a cycle
        self.points_number_in_a_cycle = 0
        self.history_points_number_in_a_cycle = []

        # record the number of sstables and points to rewrite in each cycle
        # each element is a list of 5 elements, that is
        # 1. number of merge sorted sstables
        # 2. number of direct flushed sstables
        # 3. number of the points in those merge sorted sstables
        # 4. number of the points in those direct flushed sstables
        # 5. sum of 3 and 4
        self.history_rewrite_sstable_and_point_number = []

        # record the write amplification rate of each cycle
        self.history_write_amplification_rate = []

    def __is_sequential(self, val):
        """
        Check whether a data point is sequential or not
        :param val: the generate time of a data poont
        :return: is sequential or not
        """
        return val > self.max_generate_time_on_level_1

    def write(self, val, outPutFile=None):
        """
        Write a data point to the tLSM structure
        :param val: generate time of the data point
        """
        self.points_number_in_a_cycle += 1
        if self.__is_sequential(val):
            self.sequential_buffer.append(val)
            if len(self.sequential_buffer) == self.sequential_buffer_size:
                self.__write_sequential_buffer()
        else:
            self.nonsequential_point_number_when_sequential_buffer_is_full += 1
            self.nonsequential_buffer.append(val)
            if len(self.nonsequential_buffer) == self.nonsequential_buffer_size:
                self.__write_nonsequential_buffer(outPutFile)

    def __write_nonsequential_buffer(self, outputFile=None):
        """
        Write the content inside nonsequential buffer to LEVEL1, and finally clear the nonsequential buffer
        """
        if len(self.nonsequential_buffer) > 0:
            if outputFile is not None:
                outputFile.write(str(self.max_generate_time_on_level_1)+'\n')
                outputFile.write(str(self.nonsequential_buffer)+'\n')

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

            # end of a cycle, update observation list
            self.history_sequential_buffer_flush_times_per_cycle.append(
                self.sequential_buffer_flush_times_per_cycle)
            self.sequential_buffer_flush_times_per_cycle = 0
            self.history_points_number_in_a_cycle.append(self.points_number_in_a_cycle)
            self.points_number_in_a_cycle = 0

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
            self.write_times += len(sstable.data_list)
            self.level_1.append(sstable)
            self.sequential_buffer.clear()

            # update observed records
            self.sequential_buffer_flush_times_per_cycle += 1
            self.history_nonsequential_point_number_when_sequential_buffer_if_full.append(
                self.nonsequential_point_number_when_sequential_buffer_is_full)
            self.nonsequential_point_number_when_sequential_buffer_is_full = 0

    def __merge(self, new_sstable):
        """
        Merge a new sstable to LEVEL1
        :param new_sstable: the new sstable to be merged to LEVEL1
        """
        # record the sstables need to participate in merge sort
        # because this method is called when nonsequential buffer if to be flushed, merge_list cannot be empty
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

        # collect statistical data
        merge_sorted_sstable_number, direct_flushed_sstable_number, merge_sorted_points_number, \
        direct_flushed_points_number = count(merge_list)
        self.history_rewrite_sstable_and_point_number.append(
            [merge_sorted_sstable_number, direct_flushed_sstable_number, merge_sorted_points_number,
             direct_flushed_points_number, merge_sorted_points_number + direct_flushed_points_number])
        self.history_write_amplification_rate.append(
            (merge_sorted_points_number + direct_flushed_points_number) / self.points_number_in_a_cycle)

        # append the new sstable to the merge list, apply merge sort generate several sstables, and append
        # the at the end of LEVEL1
        merge_list.append(new_sstable)
        self.write_times += count_point(merge_list)
        self.level_1.extend(merge_sort(merge_list, self.sstable_size))

    def get_write_amplification(self):
        """
        Sum the write times of all data points in LEVEL1, which is the write amplification
        :return: the sum of write times of all data points in LEVEL1
        """
        # total write times of all points
        write_times = 0
        # totally how many points
        point_number = 0
        for sstable in self.level_1:
            point_number += len(sstable.data_list)
            write_times += sstable.get_write_times()
        return point_number, write_times

    def flush(self):
        self.__write_sequential_buffer()
        self.__write_nonsequential_buffer()
