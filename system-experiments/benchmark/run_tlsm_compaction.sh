./stop_server.sh
time=$(date "+%Y%m%d-%H%M%S")
nohup ../iotdb-add_tlsm_compaction/distribution/target/apache-iotdb-0.12.1-SNAPSHOT-all-bin/apache-iotdb-0.12.1-SNAPSHOT-all-bin/sbin/start-server.sh > tlsm_compaction_$1_$2_server.log &
