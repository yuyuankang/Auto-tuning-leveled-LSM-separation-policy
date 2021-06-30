import numpy as np
import sys

arg_start_timestamp = 1622476800000
arg_interval = 20
arg_total_num = 10000000


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
    time = arg_start_timestamp
    counter = 0
    name = 'raw_data_interval_' + str(arg_interval) + '_amount_' + str(arg_total_num) + '.txt'
    with open(name) as out_file:
        while counter < arg_total_num:
            key = time
            counter += 1
            out_file.write(str(key)+','+str(time)+','+np.random.randn())
            time += arg_interval

            if counter % 1000 == 0:
                progress(float(counter / arg_total_num))
