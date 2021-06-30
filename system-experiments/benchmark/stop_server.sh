shell_pids=`pgrep run_test.sh`
iotdb_pids=`lsof -t -i:6667`
write_pids=`pgrep write_iotdb-1.0-SNAPSHOT.jar`
jps_pids=`jps -q`
echo '601tif'|sudo -S kill -9 ${shell_pids}
echo '601tif'|sudo -S kill -9 ${write_pids}
echo '601tif'|sudo -S kill -9 ${iotdb_pids}
echo '601tif'|sudo -S kill -9 ${jps_pids}