from realdelay import *
from sstable import SSTable


def range_float(minimum, maximum, step):
    """
    Generate an arithmetic sequence, given the minimum, maximum and step size
    :param minimum: minimal value of the arithmetic sequence (inclusive)
    :param maximum: maximal value of the arithmetic sequence (inclusive)
    :param step: difference of two adjacent value in the arithmetic sequence
    :return: a list, arithmetic sequence
    """
    result = []
    current_value = minimum
    while round(current_value,4) <= maximum:
        result.append(current_value)
        current_value += step
    return result


def generate_data_points_real_delay(time_interval, total_number, path='delay_milli.txt'):
    """
    Generate a collection of data points use real delay
    :param time_interval: interval of generate time
    :param total_number: total number of data points to generate
    :param path: path of the delay file
    :return:
    """
    gens = np.array(range_float(0, time_interval * total_number, time_interval))
    delays = np.array(RealDelay(path=path).get_delays(np.size(gens, 0)))
    recvs = gens + delays
    pairs = np.array([gens, recvs]).T
    sorted = pairs[pairs[:, 1].argsort()]
    return sorted[:, 0]

def generate_data_points_real_delay_with_delay(time_interval, total_number, path='delay_milli.txt'):
    """
    Generate a collection of data points use real delay
    :param time_interval: interval of generate time
    :param total_number: total number of data points to generate
    :param path: path of the delay file
    :return:
    """
    gens = np.array(range_float(0, time_interval * total_number, time_interval))
    delays = np.array(RealDelay(path=path).get_delays(np.size(gens, 0)))
    recvs = gens + delays
    data_points = np.array([gens, recvs, delays]).T
    sorted_data_points = data_points[data_points[:, 1].argsort()]
    return sorted_data_points

def generate_data_points_with_delay(time_interval, total_number, mu=2.54436, sigma=0.612408):
    data_points = []
    for i in range(total_number):
        generate_time = i * time_interval
        delay = np.random.lognormal(mu, sigma)
        arrival_time = delay + generate_time
        data_points.append([generate_time, arrival_time, delay])
    data_points = np.array(data_points)
    sorted_data_points = data_points[data_points[:, 1].argsort()]
    return sorted_data_points

def generate_data_points(time_interval, total_number, mu=2.54436, sigma=0.612408):
    """
    Generate a collection of data points.
    :param time_interval: interval of generate time
    :param total_number: total number of data points to generate
    :param mu: parameter of lognormal distribution function, mu
    :param sigma: parameter of lognormal distribution function, sigma
    :return: a collection data points, each data points is represented by its generate time
    """
    data_points = []
    for i in range(total_number):
        generate_time = i * time_interval
        arrival_time = np.random.lognormal(mu, sigma) + generate_time
        data_points.append([generate_time, arrival_time])
    data_points = np.array(data_points)
    sorted_data_points = data_points[data_points[:, 1].argsort()]
    collection = sorted_data_points[:, 0]
    return collection

def generate_data_points_gpd(time_interval, total_number, mu=0, sigma=0.0224,ksi=-0.2):
    data_points = []
    for i in range(total_number):
        generate_time = i * time_interval
        arrival_time = np.random.pareto(mu, sigma) + generate_time
        data_points.append([generate_time, arrival_time])
    data_points = np.array(data_points)
    sorted_data_points = data_points[data_points[:, 1].argsort()]
    collection = sorted_data_points[:, 0]
    return collection





def merge_sort(sstables, size_=None):
    """
    Apply merge sort algorithm to
    :param sstables: a list of sstables to be merged
    :param size_: the capacity of the output sstables
    :return: a list of sstables, as a whole is a sorted structure of data points, ordered by the generate time
    """
    # if the output sstable size is not specified, we use the capacity of the first sstable in the input list to set
    # the capacity of the output sstable
    size = len(sstables[0].data_list) if size_ is None else size_
    # the output sstable list
    result_list = []

    if len(sstables) == 1:
        result_list.append(sstables[0])
        return result_list

    merged_data = []
    min_ = -1
    max_ = -1

    # collect the data point with minimal generate time of all sstables
    minimal_values = []
    for sstable in sstables:
        minimal_values.append(sstable.peek())
    nan_num = 0
    counter = 0

    while True:
        if nan_num == len(sstables):
            # all sstables to be merged is empty
            break
        min_index = int(np.nanargmin(minimal_values))
        if min_ == -1:
            min_ = minimal_values[min_index]
        max_ = minimal_values[min_index]
        merged_data.append(sstables[min_index].pop())
        counter += 1
        if counter == size:
            result_list.append(SSTable(merged_data, min_, max_, 0, True))
            counter = 0
            merged_data = []
            min_ = -1
        updated = sstables[min_index].peek()
        if updated is np.nan:
            nan_num += 1
        minimal_values[min_index] = updated
    if len(merged_data) > 0:
        result_list.append(SSTable(merged_data, min_, max_, 0, True))
    return result_list
