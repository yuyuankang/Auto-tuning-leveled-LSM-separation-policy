from lsm import LSM
import numpy as np
from tlsm import tLSM
import sys

np.set_printoptions(threshold=sys.maxsize)

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



line_num = 1024

buffer_size = 5120
sequential_buffer_size = 1000
nonsequential_buffer_size = buffer_size - sequential_buffer_size

tlsm = tLSM(sequential_buffer_size, nonsequential_buffer_size, buffer_size)
counter = 0

output_file = open('out-of-order-buffer.txt','w')

with open('data_for_iotdb_ty_10000000_50.csv') as file:
    lines = file.readlines(line_num)
    while lines:
        for line in lines:
            arrivalTimeStr, generationTimeStr, valueStr = line.split(',')
            arrivalTime = int(arrivalTimeStr)
            generationTime = int(generationTimeStr)
            tlsm.write(generationTime, output_file)
            value = int(valueStr)
        counter += len(lines)
        progress(float(counter / 10000000))
        lines = file.readlines(line_num)

tlsm.flush()

output_file.close()

with open('confirm_result.txt', 'w') as file_out:
    for sstable in tlsm.level_1:
        file_out.write(str(sstable.data_list) + '\n')

    merge_num_list_old_files = np.array(tlsm.history_rewrite_sstable_and_point_number)[:, 0]
    merge_num_list_new_files = np.array(tlsm.history_rewrite_sstable_and_point_number)[:, 1]
    merge_num_list = merge_num_list_old_files + merge_num_list_new_files
    file_out.write(str(merge_num_list))


