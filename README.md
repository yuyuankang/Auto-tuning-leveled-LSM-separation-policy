# Auto-tuning-leveled-LSM-separation-policy

We explore the separation policy of order and out-of-order data when writing to LSM-tree.

In `data-generation`, we provide the real-world delay set and the method to generate dataset for experiments based on it.

In `simulation-experiments`, we provide the implementations of LSM-tree with and without separation policy. The necessary codes for each experiment are listed. 

In `system-experiments`, we provide the implementations in IoTDB (with/without separation policy). All necessary experiment scripts are shown. 