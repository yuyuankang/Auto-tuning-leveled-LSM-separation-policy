./stop_server.sh
nohup ../iotdb-add_lsm_compaction/distribution/target/apache-iotdb-0.12.1-SNAPSHOT-all-bin/apache-iotdb-0.12.1-SNAPSHOT-all-bin/sbin/start-server.sh > tlsm_$1_$2_query.log &
sleep 5
java -jar read_iotdb-1.0-SNAPSHOT.jar query/$2.txt
