#!/bin/bash
data_file_list=(data_for_iotdb_ty_10000000_50.csv data_for_iotdb_ty_10000000_100.csv data_for_iotdb_ty_10000000_500.csv data_for_iotdb_ty_10000000_1000.csv data_for_iotdb_ty_10000000_5000.csv)
point_num_list=(10000000 10000000 10000000 10000000 10000000)
time_interval_list=(50 100 500 1000 5000)
tlsm_list=(1000 2000 3000 4000)
for i in 0 1 2 3 4; do
    echo "${point_num_list[i]}_${time_interval_list[i]}"
    cp lsm_write.properties ../iotdb-add_lsm_compaction/distribution/target/apache-iotdb-0.12.1-SNAPSHOT-all-bin/apache-iotdb-0.12.1-SNAPSHOT-all-bin/conf/iotdb-engine.properties
    ./run_lsm.sh ${data_file_list[i]} ${point_num_list[i]} ${time_interval_list[i]}
    du -smh ../iotdb-add_lsm_compaction/distribution/target/apache-iotdb-0.12.1-SNAPSHOT-all-bin/apache-iotdb-0.12.1-SNAPSHOT-all-bin/data
    cp lsm_compaction.properties ../iotdb-add_lsm_compaction/distribution/target/apache-iotdb-0.12.1-SNAPSHOT-all-bin/apache-iotdb-0.12.1-SNAPSHOT-all-bin/conf/iotdb-engine.properties
    ./run_lsm_compaction.sh ${point_num_list[i]} ${time_interval_list[i]} &&
    old_compaction_log_status="`ls -l ../iotdb-add_lsm_compaction/distribution/target/apache-iotdb-0.12.1-SNAPSHOT-all-bin/apache-iotdb-0.12.1-SNAPSHOT-all-bin/data/data/unsequence/root.storage_group/0/0/ | wc -l`"
    while [[ true ]]; do
        sleep 10s
        new_compaction_log_status="`ls -l ../iotdb-add_lsm_compaction/distribution/target/apache-iotdb-0.12.1-SNAPSHOT-all-bin/apache-iotdb-0.12.1-SNAPSHOT-all-bin/data/data/unsequence/root.storage_group/0/0/ | wc -l`"
        if [[ `echo ${old_compaction_log_status}` == `echo ${new_compaction_log_status}` ]]; then
            break
        fi
        old_compaction_log_status=$new_compaction_log_status
    done
    sleep 5
    ./query_lsm.sh ${point_num_list[i]} ${time_interval_list[i]}
    du -smh ../iotdb-add_lsm_compaction/distribution/target/apache-iotdb-0.12.1-SNAPSHOT-all-bin/apache-iotdb-0.12.1-SNAPSHOT-all-bin/data
done
for i in 0 1 2 3 4; do
    for j in 0 1 2 3; do
        echo "${point_num_list[i]}_${time_interval_list[i]}"
        echo "tlsm_seq_memtable_size: ${tlsm_list[j]}"
        cp tlsm_write_${tlsm_list[j]}.properties ../iotdb-add_tlsm_compaction/distribution/target/apache-iotdb-0.12.1-SNAPSHOT-all-bin/apache-iotdb-0.12.1-SNAPSHOT-all-bin/conf/iotdb-engine.properties
        ./run_tlsm.sh ${data_file_list[i]} ${point_num_list[i]} ${time_interval_list[i]}
        du -smh ../iotdb-add_tlsm_compaction/distribution/target/apache-iotdb-0.12.1-SNAPSHOT-all-bin/apache-iotdb-0.12.1-SNAPSHOT-all-bin/data
        cp tlsm_compaction_${tlsm_list[j]}.properties ../iotdb-add_tlsm_compaction/distribution/target/apache-iotdb-0.12.1-SNAPSHOT-all-bin/apache-iotdb-0.12.1-SNAPSHOT-all-bin/conf/iotdb-engine.properties
        ./run_tlsm_compaction.sh ${point_num_list[i]} ${time_interval_list[i]} &&
        old_compaction_log_status="`ls -l ../iotdb-add_tlsm_compaction/distribution/target/apache-iotdb-0.12.1-SNAPSHOT-all-bin/apache-iotdb-0.12.1-SNAPSHOT-all-bin/data/data/unsequence/root.storage_group/0/0/ | wc -l`"
        while [[ true ]]; do
            sleep 10s
            new_compaction_log_status="`ls -l ../iotdb-add_tlsm_compaction/distribution/target/apache-iotdb-0.12.1-SNAPSHOT-all-bin/apache-iotdb-0.12.1-SNAPSHOT-all-bin/data/data/unsequence/root.storage_group/0/0/ | wc -l`"
            if [[ `echo ${old_compaction_log_status}` == `echo ${new_compaction_log_status}` ]]; then
                break
            fi
            old_compaction_log_status=$new_compaction_log_status
        done
        sleep 5
        ./query_tlsm.sh ${point_num_list[i]} ${time_interval_list[i]}
        du -smh ../iotdb-add_tlsm_compaction/distribution/target/apache-iotdb-0.12.1-SNAPSHOT-all-bin/apache-iotdb-0.12.1-SNAPSHOT-all-bin/data
    done
done