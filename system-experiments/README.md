# Directories

The experiments are based on 

iotdb-add_lsm_compaction：Apache IoTDB V0.12, introduced LSM-tree leveling merge without separation of in-order and out-of-order data
iotdb-add_tlsm_compaction：the separation policy is implemented in Apache IoTDB V0.12

benchmark：data generation、data writing、testing

# Benchmark

There are 5 parts in benchmark

### Data Generation

ty.txt：a delay set collected from real-world scenario

data_prepare_iotdb_ty.py：generate data according to ty.txt ( in form of csv, which will be used later）

```powershell
python data_prepare_iotdb_ty.py ${point-num} ${time-interval}
```
generate_data.sh：executes data_prepare_iotdb_ty.py to generate data

### IoTDB Configuration

lsm_write.properties：LSM-tree parameters when writing IoTDB

lsm_compaction.properties：LSM-tree parameters for compaction

tlsm_write_1000.properties：parameters for writing when ordered memtable is set to be 1000

tlsm_compaction_1000.properties：parameters for compaction when ordered memtable is 1000

tlsm_write_2000.properties：parameters for writing when ordered memtable is set to be 2000

tlsm_compaction_2000.properties：parameters for compaction when ordered memtable is set to be 2000

tlsm_write_3000.properties：parameters for writing when ordered memtable is set to be 3000

tlsm_compaction_3000.properties：parameters for compaction when ordered memtable is set to be 3000

tlsm_write_4000.properties：parameters for writing when ordered memtable is set to be 4000

tlsm_compaction_4000.properties：parameters for compaction when ordered memtable is set to be 4000

### Writing

write_iotdb-1.0-SNAPSHOT.jar: for writing data to IoTDB

```powershell
java -jar write_iotdb-1.0-SNAPSHOT.jar 127.0.0.1 6667 root.storage_group ${csv-file}
```
run_lsm.sh：shell for writing data to LSM-treelsm, without separation policy
```powershell
./run_lsm.sh ${csv-file} ${points-num} ${time-interval}
```
run_lsm_compaction：shell for LSM-tree compaction, without separation policy
```powershell
./run_lsm_compaction.sh ${points-num} ${time-interval}
```
run_tlsm.sh：shell for writing data to LSM-treelsm, with separation policy
```powershell
./run_tlsm.sh ${csv-file} ${points-num} ${time-interval}
```
run_tlsm_compaction：shell for LSM-tree compaction, with separation policy
```powershell
./run_tlsm_compaction.sh ${points-num} ${time-interval}
```
<!-- ### Querying

query文件夹：查询所需的所有sql语句

read_iotdb-1.0-SNAPSHOT.jar：iotdb查询数据脚本

```powershell
java -jar read_iotdb-1.0-SNAPSHOT.jar ${需要查询的sql语句文件(即query文件夹中的任意一个文件)}
```
query_lsm.sh：lsm查询调度脚本
```powershell
./query_lsm.sh ${生成点数} ${乱序时间间隔}
```
query_tlsm.sh：tlsm查询调度脚本
```powershell
./query_tlsm.sh ${生成点数} ${乱序时间间隔}
``` -->
### Overall experiment running

stop_server.sh：terminates all experiments (including processes of IoTDB)

run_test.sh：running the experiments (execluding data generation)

# Starting process in the paper

### Apache IoTDB Compilation

```plain
cd iotdb-add_tlsm_compaction
mvnclean package -DskipTests //waiting for compilation
cd ../iotdb-add_lsm_compaction
mvn clean package -DskipTests //waiting for compilation
```
### Data generation

```plain
cd ../benchmark
./generate_data.sh //waiting for data generation
```
### Running Tests

```plain
nohup ./run_test.sh > data.log &
```
After running all of the experiments, analysis the data in data.log to get write amplification.
