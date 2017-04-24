#!/bin/bash
#https://issues.apache.org/jira/browse/HBASE-12866
# parameters for assigning input and output dir
script_root_dir="`pwd`/../"
additional_conf="${script_root_dir}/conf/winter-hbase.conf"
hbase_nodes="${script_root_dir}/conf/hbase-nodes"
client_hosts_file="${script_root_dir}/conf/clients"
scan_filter_dir="${script_root_dir}/conf/filters"
output_dir="${script_root_dir}/output/"
remote_log_dir="${output_dir}/remote-log"
perf_test_dir="${output_dir}/perf-test"
dstat_tmp_dir="/tmp/dstat/"

table_name="tbl_x"
stat_file_name="stat.dat"
hbase_op_script="./hbase-op.sh"
hbase_insert_script="./insert-server.sh"
hbase_perf_insert_script="./perf-server.sh"
parse_result_script="./parse-results.sh"
expect_flush_script="./expect-flush.sh"
data_dir_home="/home/`whoami`/data"
#data_dir_home="/data/chfeng/data"
#data_dir_ditb="${data_dir_home}/ad-data/to-client"
data_dir_ditb="${data_dir_home}/ditb-out"
data_dir_hadoop="${data_dir_home}/hadoop-data/"
data_dir_hbase_lcindex="${data_dir_home}/hbase-data/lcindex"

#bin_dir="/home/fengchen/scripts/bin/code-of-test/"
#bin_dir="/data/chfeng/scripts/ad/bin/code-of-test/"
bin_dir="/home/winter/workspace/intelliJ/hbase-1.2.1-lcindex/out/production/code-of-test"
cls_name_generate_data=ditb.put.DITBDataGenerator
cls_name_scan=ditb.scan.DITBScan
cls_name_perf_generator=ditb.perf.PerfDataGenerator
cls_name_perf_scan=ditb.perf.PerfScanBase

# workload_desc_path must be full path to be used in Java Program
cls_name_workload=ditb.workload.AdWorkload
workload_desc_path="/home/winter/workspace/git/LCIndex-HBase-1.2.1/scripts/ditb/conf/workload-ad"
#cls_name_workload=ditb.workload.UniWorkload
#workload_desc_path="/home/winter/workspace/git/LCIndex-HBase-1.2.1/scripts/ditb/conf/workload-uni"
#cls_name_workload=ditb.workload.TPCHWorkload
#workload_desc_path="/home/winter/workspace/git/LCIndex-HBase-1.2.1/scripts/ditb/conf/workload-tpch"

# configurable parameters for running java programs
no_process=3
no_thread=10
# configurable parameters for testing performance on different tables
nb_record=10000
nb_get=1000
nb_scan=10
size_scan_covering=10
performance_test_times=1

# check param and dirs
mkdir -p ${output_dir} ${dstat_tmp_dir}
if [ "${HADOOP_HOME}x" = "x" ]; then
    echo "set {HADOOP_HOME} at first"
    exit
elif [ "${HBASE_HOME}x" = "x" ]; then
    echo "set {HBASE_HOME} at first"
    exit
fi

# load cls_path
cls_path="${bin_dir}"
for f in ${HADOOP_HOME}/share/hadoop/*/*.jar; do
	cls_path=${cls_path}:$f
done
for f in ${HBASE_HOME}/lib/*.jar; do
	cls_path=${cls_path}:$f
done

start_dstat() {
    for host in `cat ${hbase_nodes}`;do
        ssh ${host} "rm -r "${dstat_tmp_dir}"; mkdir -p ${dstat_tmp_dir}"
        ssh ${host} "nohup dstat --time --cpu --mem --disk --net --output ${dstat_tmp_dir}/DSTAT_${host}.csv > /dev/null" &> /dev/null &
    done
}

# p1=target_dir
stop_dstat() {
    DSTAT_LOG=/tmp/dstat-local-`date +%Y%m%d-%H%M%S`
    mkdir -p $DSTAT_LOG
	# copy to local
    for host in `cat ${hbase_nodes}`;do
        echo "cp data from "$i" to "$1
        scp -r -q ${host}:${dstat_tmp_dir} ${DSTAT_LOG}
    done
	# kill dstats on remote server
	for host in `cat ${hbase_nodes}`;do
		for pid in `ssh ${host} "ps aux | grep DSTAT" | awk '{print $2}'` ;do
			ssh ${host} "kill -9 ${pid}";
		done
	done
    mv ${DSTAT_LOG} ${1}
}

fun_CopyConfAndScript(){
    for host in `cat ${client_hosts_file}`; do
        ssh $host "mkdir -p ${script_root_dir}"
        ssh $host "mkdir -p ${bin_dir}/../"
        scp -rq ${bin_dir} ${host}:${bin_dir}/../
        scp -rq ${script_root_dir}/scripts ${host}:${script_root_dir}
        scp -rq ${script_root_dir}/conf ${host}:${script_root_dir}
    done
}

fun_GenerateData(){
    # data_dir($1) stat_file_name(default) client_hosts_file(default) no_process($2) no_thread($3)
    java -cp ${cls_path} ${cls_name_generate_data} $1 ${stat_file_name} ${client_hosts_file} $2 $3 ${cls_name_workload} ${workload_desc_path}
}

fun_Insert(){
    # cls_path(default) index_type($1) data_input_dir($2) stat_file_name(default) client_hosts_file(default) no_process($3) no_thread($4) cls_name_workload workload_desc_path now($5) 
    bash ${hbase_insert_script} ${cls_path} $1 $2 ${stat_file_name} ${client_hosts_file} $3 $4 ${cls_name_workload} ${workload_desc_path} $5
}

fun_PerfInsert(){
    # cls_path(default) index_type($1) data_input_dir($2) stat_file_name(default) client_hosts_file(default) no_process($3) no_thread($4) cls_name_workload workload_desc_path now($5) 
    bash ${hbase_perf_insert_script} ${cls_path} $1 $2 ${stat_file_name} ${client_hosts_file} $3 $4 ${cls_name_workload} ${workload_desc_path} $5
}

fun_Scan(){
	echo "running scan for $1, filters in $2"
#	for file in `ls $2`; do
		# index_type($1) cls_name_workload workload_desc_path
    	java -cp ${cls_path} ${cls_name_scan} $1 ${cls_name_workload} ${workload_desc_path}
#	done	
}

fun_CalStorageCost(){
	# output_path($1) index_type($2)
	cat /dev/null > $1
    for host in `cat ${hbase_nodes}`; do
        ssh ${host} "du -s ${data_dir_hadoop}" | awk '{print $1 " " $2}' | tee -a $1
		if [ "${2}x" = "LCIndexx" ]; then
        	ssh ${host} "du -s ${data_dir_hbase_lcindex}" | awk '{print $1 " " $2}' | tee -a $1
		fi
    done
}

fun_PrintStorageCost(){
    sum=0
    for host in `cat ${hbase_nodes}`; do
        count=`ssh ${host} "du -s ${data_dir_hadoop}" | awk '{print $1}' `
        sum=$(($sum+$count))
    done
    echo "total storage cost: $sum"
}

fun_CopyLogs(){
	# output_dir($1) no_process($2) no_thread($3) suffix($4) index_type($5) work_type($6), see ./insert-server.sh and ./insert-client.sh
    mkdir -p $1/logs-hdfs-$6
    bash ${hbase_op_script} copyhdfs $1/logs-hdfs-$6
    mkdir -p $1/logs-hbase-$6
    bash ${hbase_op_script} copyhbase $1/logs-hbase-$6
    bash ${hbase_op_script} clearlog
    echo "copy logs next"
	mkdir -p ${1}/remote-log
    for host in `cat ${client_hosts_file}`; do
        scp -rq ${host}:"${remote_log_dir}/${host}-*-${3}-${4}-${5}.log" $1/remote-log
		ssh ${host} "rm ${remote_log_dir}/${host}-*-${3}-${4}-${5}.log"
    done
}

fun_ReGeneratedData(){
    for host in `cat ${client_hosts_file}`; do
        ssh $host "rm -rf ${data_dir_ditb}"
    done
	rm -rf ${data_dir_ditb}
	fun_GenerateData ${data_dir_ditb} ${no_process} ${no_thread}
}

fun_ReGeneratedDataAndPerf(){
    for host in `cat ${client_hosts_file}`; do
        ssh $host "rm -rf ${data_dir_ditb}"
    done
	rm -rf ${data_dir_ditb}
	fun_GenerateData ${data_dir_ditb} ${no_process} ${no_thread}
	java -cp ${cls_path} ${cls_name_perf_generator} ${data_dir_ditb} ${nb_record} ${nb_get} ${nb_scan}
}

fun_GetScanCompare(){
    # conf_path(default) test_times(default) total_size(default) op_size(default)
    java -cp ${cls_path} ${cls_name_getscancmp} ${additional_conf} ${getscan_runtime} ${getscan_total_size} ${getscan_op_size}
}

fun_ClearNonData(){
	stop-hbase.sh
	hdfs dfs -rm -r -f /hbase/WALs
	hdfs dfs -rm -r -f /hbase/archive
	hdfs dfs -rm -r -f /hbase/oldWALs
	sleep 20
}


fun_ParallelPerf() {
    now=`date +%Y%m%d-%H%M%S`
    #for index_type in NoIndex PF_CCT_ PF_CCIT PF_SCND PF_MSND; do
    #for index_type in NoIndex PF_CCT_ PF_CCIT PF_SCND PF_MBKT PF_MSND PF_BKRW ; do
	for index_type in PF_BKRW ; do
        echo "running "${index_type}" at ${now}"
        cur_output=${output_dir}/perf-${now}-${index_type}
        mkdir -p ${cur_output}
        # copy conf, bins and scripts to client
#        fun_CopyConfAndScript
        # kill, clear and restart hdfs/hbase
        bash ${hbase_op_script} kcs
        # run insert
        fun_PerfInsert ${index_type} ${data_dir_ditb} ${no_process} ${no_thread} ${now} 2>&1 | tee ${cur_output}/insert.log 
		# manually flush to hfile at first
#		${expect_flush_script} ${table_name}
		# force flush all tables by stopping hbase
#		stop-hbase.sh && sleep 10 && start-hbase.sh && sleep 10
        # run scan 
		java -cp ${cls_path} ${cls_name_perf_scan} ${index_type} ${cls_name_workload} ${workload_desc_path} ${data_dir_ditb} ${nb_get} ${nb_scan} ${size_scan_covering} ${performance_test_times} 2>&1 | tee ${cur_output}/scan.log
        fun_CopyLogs ${cur_output} ${no_process} ${no_thread} ${now} ${index_type} perf
    done
	bash ${parse_result_script} ${now} "perf-${cls_name_workload}"
	echo "perf all has done, check results"
}

fun_Run() {
    now=`date +%Y%m%d-%H%M%S`
    for index_type in NoIndex GSIndex CCIndex LCIndex IRIndex MDIndex; do
#	for index_type in NoIndex GSIndex; do
#	for index_type in NoIndex LMDIndex_D LMDIndex_S; do
        echo "running "${index_type}" at ${now}"
        cur_output=${output_dir}/normal-${now}-${index_type}
        mkdir -p ${cur_output}
        # copy conf, bins and scripts to client
        #fun_CopyConfAndScript
        # kill clear and restart hdfs/hbase
        bash ${hbase_op_script} kc && sleep 20 && start-dfs.sh && sleep 20 && start-hbase.sh && sleep 20
        # run insert
        fun_Insert ${index_type} ${data_dir_ditb} ${no_process} ${no_thread} ${now} 2>&1 | tee ${cur_output}/insert.log 
		# manually flush to hfile at first
		#${expect_flush_script} ${table_name}
		# force flush all tables by stopping hbase
		#stop-hbase.sh && sleep 10 
        #fun_CopyLogs ${cur_output} ${no_process} ${no_thread} ${now} ${index_type} insert
		# start hbase to run scan
		#start-hbase.sh && sleep 10
        # run scan 
        #fun_Scan ${index_type} 2>&1 | tee ${cur_output}/scan.log
        # calculate storage cost
		#fun_ClearNonData
        #fun_CalStorageCost ${cur_output}/storage ${index_type}
        # save logs and copy to local
        fun_CopyLogs ${cur_output} ${no_process} ${no_thread} ${now} ${index_type} scan
    done
	bash ${parse_result_script} ${now} "put-${cls_name_workload}"
	echo "all has done, check results"
}

# generate data, parse src file into sub-files for clients-threads
#fun_ReGeneratedData
fun_Run

#fun_Scan NoIndex
#fun_ReGeneratedDataAndPerf
#fun_ParallelPerf
