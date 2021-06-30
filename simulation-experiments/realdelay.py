import numpy as np

np.random.seed(4834)


class RealDelay:
    def __init__(self, path='delay_milli.txt', line_num=1024) -> None:
        super().__init__()
        self.delay_data = []
        with open(path, 'r') as delay_file:
            lines = delay_file.readlines(line_num)
            while lines:
                vals = list(map(int, lines))
                self.delay_data += vals
                lines = delay_file.readlines(line_num)

    def get_delays(self, num=1):
        return np.random.choice(self.delay_data, num)
