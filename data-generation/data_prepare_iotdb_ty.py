from queue import PriorityQueue
import random
random.seed(4834)
import sys


def triple2line(triple, delimiter=','):
    """
    Transform a triple to a line of string. The value are concatenated with given delimiter.
    :param triple: a triple containing three float values
    :param delimiter: delimiter for concatenating the values
    :return: concatenated string, ending with '\n'
    """
    return str(triple[0]) + delimiter + str(triple[1]) + delimiter + str(triple[2]) + '\n'


def progress(percent, width=100):
    """
    Show the progress of data generation
    :param percent: percentage of tasks completed
    :param width: The width of the progress bar
    """
    if percent > 1:
        percent = 1
    show_str = ('[%%-%ds]' % width) % (int(percent * width) * '#')
    print('\r%s %s%%' % (show_str, int(percent * 100)), end='', file=sys.stdout, flush=True)


def get_delay_set(path, line_number=1024):
    delay_data = []
    with open(path, 'r') as delay_file:
        lines = delay_file.readlines(line_number)
        while lines:
            vals = list(map(int, lines))
            delay_data += vals
            lines = delay_file.readlines(line_number)
    return delay_data


if __name__ == '__main__':

    data_points_number = 100000000
    time_interval = 100
    buffer_size = 50000

    # path of the output file
    output_file_name = 'data_for_iotdb_ty.csv'

    buffer = PriorityQueue(buffer_size)
    max_arrival_time_in_file = -1
    # number of data points written to the file
    current_num = 0

    delays = get_delay_set('ty.txt')
    with open(output_file_name, 'w') as file_out:
        # generate data points
        for i in range(data_points_number):
            gen_time = int(i * time_interval)
            delay = random.sample(delays, 1)[0]
            arrival_time = gen_time + delay
            value = random.randint(0, 1000000)
            if buffer.full():
                point = buffer.get()
                if point[0] < max_arrival_time_in_file:
                    print('drop a data point', str(point), 'because arrival time is too early.')
                    continue
                file_out.write(triple2line(point))
                max_arrival_time_in_file = point[0]
                current_num += 1
                if current_num % 1000 == 0:
                    progress(float(current_num / data_points_number))
            buffer.put((arrival_time, gen_time, delay, value))

        while not buffer.empty():
            point = buffer.get()
            if point[0] < max_arrival_time_in_file:
                print('drop a data point', str(point), 'because arrival time is too early')
                continue
            file_out.write(triple2line(point))
            max_arrival_time_in_file = point[0]
            current_num += 1
            if current_num % 1000 == 0:
                progress(float(current_num / data_points_number))
