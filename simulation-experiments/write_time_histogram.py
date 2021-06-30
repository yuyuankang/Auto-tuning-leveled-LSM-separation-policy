from algorithm_utils import *
from lsm import LSM
from tlsm import tLSM
from write_amplify import *

np.random.seed(4834)

arg_interval = 2
arg_buffer_size = 512

arg_seq_buffer_size = 256
arg_nonseq_buffer_size = arg_buffer_size - arg_seq_buffer_size

arg_total_num = 100000
arg_mu = 4
arg_sigma = 1.5

if __name__ == '__main__':
    data_points = generate_data_points(arg_interval, arg_total_num, arg_mu, arg_sigma)
    # real_data_points = generate_data_points_real_delay(arg_interval, arg_total_num, path='ty.txt')

    print('data is prepared')

    lsm = LSM(arg_buffer_size)
    tlsm = tLSM(arg_seq_buffer_size, arg_nonseq_buffer_size)

    for point in data_points:
        lsm.write(point)
        tlsm.write(point)

    lsm_info = r'$\Delta t$=' + str(arg_interval) + ', $n$=' + str(arg_buffer_size)
    tlsm_info = r'$\Delta t$=' + str(arg_interval) + ', $n_1$=' + str(arg_seq_buffer_size) + ', $n_2$=' + str(
        arg_nonseq_buffer_size)
    analysis_write(lsm, show_hist=True, title=lsm_info,
                   file_name='lsm_' + str(arg_interval) + '_' + str(arg_mu) + '_' + str(arg_sigma))
    analysis_write(tlsm, show_hist=True, title=tlsm_info,
                   file_name='tlsm_' + str(arg_interval) + '_' + str(arg_mu) + '_' + str(arg_sigma))


    # arg_seq_buffer_size = 128
    # arg_nonseq_buffer_size = arg_buffer_size - arg_seq_buffer_size
    # lsm = LSM(arg_buffer_size)
    # tlsm = tLSM(arg_seq_buffer_size, arg_nonseq_buffer_size)

    # for point in real_data_points:
    #     lsm.write(point)
    #     tlsm.write(point)
    #
    # lsm_info = r'$\Delta t$=' + str(arg_interval) + ', $n$=' + str(arg_buffer_size)
    # tlsm_info = r'$\Delta t$=' + str(arg_interval) + ', $n_1$=' + str(arg_seq_buffer_size) + ', $n_2$=' + str(
    #     arg_nonseq_buffer_size)
    # analysis_write(lsm, show_hist=True, title=lsm_info, file_name='lsm_' + str(arg_interval) + '_real_delay',
    #                max_y=100000)
    # analysis_write(tlsm, show_hist=True, title=tlsm_info,
    #                file_name='tlsm_' + str(arg_interval) + '_real_delay', max_y=100000)
