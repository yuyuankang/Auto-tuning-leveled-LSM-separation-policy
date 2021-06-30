import numpy as np

import matplotlib.pyplot as plt


def get_write_amplification(lsm):
    times = []
    for sstable in lsm.level_1:
        for pair in sstable.data_list:
            times.append(pair[1])
    return times


def analysis_write(lsm, title=None, file_name=None, show_hist=False, max_y=80000):
    times = get_write_amplification(lsm)
    if show_hist:
        var = np.round(np.var(times), 2)
        mean = np.round(np.mean(times))
        all_times = np.sum(times)
        fig = plt.figure(figsize=(8, 6))
        ax = fig.add_subplot(111)
        plt.xticks(fontsize=30)
        plt.yticks(fontsize=30)
        ax.hist(times, 12, range=[0, 12])
        # ax.hist(times, 100, [0, 100])

        axes = plt.gca()
        axes.set_ylim([0, max_y])
        if title is not None:
            ax.set_title(title + ', \n sum:' + str(all_times), fontsize=30)
        plt.tight_layout()
        xsticks = [0.5, 2.5, 4.5, 6.5, 8.5, 10.5, 12.5]
        xsticks_labels = ['0', '2', '4', '6', '8', '10', '12']
        plt.xticks(xsticks, xsticks_labels)
        if file_name is not None:
            plt.savefig(file_name + '.pdf')
        fig.tight_layout()
        plt.show()
    # print('sum:', np.sum(times), 'mean:', np.mean(times), 'var:', np.var(times))
    return np.sum(times), np.mean(times), np.var(times)
