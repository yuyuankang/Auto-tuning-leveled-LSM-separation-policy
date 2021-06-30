import numpy as np


class SSTable:
    def __init__(self, data_list_, min_, max_, index_, is_merge_sorted=False):
        """
        Construct an SSTable
        :param data_list_: a list of the data points, where the data points are ordered by generate time
        :param min_: minimal key of the SSTable
        :param max_: maximal key of the SSTable
        :param index_: a pointer, indicating the valid start index of this SSTable
        :param is_merge_sorted: mark if the SSTable is generated from merge sorting or not
        """
        self.data_list = data_list_
        self.min_val = min_
        self.max_val = max_
        self.index = index_
        self.is_from_merge_sort = is_merge_sorted

    def to_string(self):
        return 'sstable:(' + str(self.is_from_merge_sort) + ')' + str(self.data_list)

    def peek(self):
        """
        To get the minimal key in the SSTable
        :return: the key of the element pointed by self.index
        """
        # if the index is out of bound, return np.nan
        if self.index >= len(self.data_list):
            return np.nan
        else:
            return self.data_list[self.index][0]

    def pop(self):
        """
        To get the element with minimal key, and remove it from SSTable
        :return: the element with minimal key, the element's write times is increased by one
        """
        if self.index > len(self.data_list):
            print('pop none')
            return None
        else:
            # the pair is <generate_time, write_times>
            pair = self.data_list[self.index]
            pair[1] += 1
            self.index += 1
            return pair

    def rewrite(self):
        """
        Rewrite the SSTable will increase the write times of all data points inside the SSTable by one
        """
        for i in range(len(self.data_list)):
            self.data_list[i][1] += 1

    def get_write_times(self):
        """
        Calculate the sum of the write times of all data points
        :return: write times sum of all data points
        """
        return sum(np.array(self.data_list)[:, 1])
