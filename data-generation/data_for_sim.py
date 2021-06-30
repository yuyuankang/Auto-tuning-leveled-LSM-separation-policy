from queue import PriorityQueue
import random

random.seed(4834)
import sys


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


if __name__ == '__main__':

    data_points_number = 10000000
    time_interval = 50

    # path of the output file
    output_file_name = 'data_for_sim.csv'

    max_arrival_time_in_file = -1
    # number of data points written to the file
    current_num = 0

    with open(output_file_name, 'w') as file_out:
        # generate data points
        for i in range(data_points_number):
            gen_time = int(i * time_interval)
            value = random.randint(0, 1000000)
            file_out.write(str(gen_time) + ', ' + str(value)+'\n')
            current_num += 1
            if current_num % 1000 == 0:
                progress(float(current_num / data_points_number))


