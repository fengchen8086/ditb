#!/bin/bash
if [ $# -lt 5 ]; then
    echo "need 5 parameters, server_hostname cur_hostname thread_id no_thread suffix"
	echo "cur: $@"
    exit
fi

# check param and dirs
if [ "${HADOOP_HOME}x" = "x" ]; then
    echo "set {HADOOP_HOME} at first"
    exit
elif [ "${HBASE_HOME}x" = "x" ]; then
    echo "set {HBASE_HOME} at first"
    exit
fi

bin_dir="/home/winter/workspace/intelliJ/hbase-1.2.1-lcindex/out/production/code-of-test"
#bin_dir="/data/chfeng/scripts/ad/bin/code-of-test"
remote_log_dir="`pwd`/../output/remote-log" # log under cur dir will not be copied to server
mkdir -p ${remote_log_dir}

# load classpath
cls_path="${bin_dir}"
for f in ${HADOOP_HOME}/share/hadoop/*/*.jar; do
    cls_path=${cls_path}:$f
done
for f in ${HBASE_HOME}/lib/*.jar; do
    cls_path=${cls_path}:$f
done

server_hostname=$1
cur_hostname=$2
process_id=$3
no_thread=$4
suffix=$5
client_log_file=${remote_log_dir}/${cur_hostname}-${process_id}-${no_thread}-${suffix}.log

cls_name_insert_client="ditb.perf.PerfClient"
JVM_PARAM='-Xmx1000m -XX:+UseConcMarkSweepGC'

echo "start perf client at ${cur_hostname}" 2>&1 | tee ${client_log_file}
nohup java $JVM_PARAM -cp ${cls_path} ${cls_name_insert_client} ${server_hostname} ${cur_hostname} ${process_id} ${no_thread} > ${client_log_file} 2>&1 &
