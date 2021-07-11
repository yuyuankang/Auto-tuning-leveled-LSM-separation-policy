# Simulation Analysis

The data structure of leveled LSM-Trees with and without separation policy are implemented in lsm.py and tlsm.py respectively.

## Histogram of Writing Times

For drawing the histograms of writing times of each data point in LSM-tree with/without separation policy,  write_time_histogram.py shows the codes.

## Model Evaluation

The experiments where $\mu=4$ and $\sigma=1.5$ can be reproduced by compares_iotdb.py.

The experiments with various delay distributions ($\mu=4, 4.5, 5$ and $\sigma=1, 1.5, 2$) can be reproduced by compares_various_distribution.py.

## Evaluation of Auto-tuning Algorithm

The auto-tuning algorithm is implemented in implement.py. The core data structure is `Hybrid`. 

## Impact on SSTable Size

To shows the sizes of SSTables after writing LSM-tree with separation policy, file_size_discuss.py shows the codes. 