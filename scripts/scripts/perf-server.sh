#!/bin/bash

if [ $# -lt 10 ]; then
    echo "need 10 parameters, cls_path index_type data_input_dir stat_file_name client_hosts_file no_process no_thread workload_cls_name workload_desc_path now_date"
	echo "now we have: $@"
    exit
fi

# cls_path(default) index_type($1) data_input_dir($2) stat_file_name(default) client_hosts_file(default) no_thread($3)
cls_path=$1
index_type=$2
data_input_dir=$3
stat_file_name=$4
client_hosts_file=$5
no_process=$6
no_thread=$7
workload_cls_name=$8
workload_desc_path=$9
now_date=${10}

# default parameters
# same as insert-server except the client script
cur_dir=`pwd`
insert_client_script="${cur_dir}/perf-client.sh"
server_host=`cat ${cur_dir}/../conf/primary`
JVM_PARAM='-Xmx1000m -XX:+UseConcMarkSweepGC'
cls_name_insert_server="ditb.put.DITBServer"

# start clients
for host in `cat ${client_hosts_file}`; do
    scp -q ${insert_client_script} ${host}:${insert_client_script}
    echo "${insert_client_script} ${server_host} ${host} ${i} ${no_thread} ${now_date}-${index_type}"
	for((i=0;i<${no_process};i++)) do
    	ssh ${host} "cd ${cur_dir} && nohup sh ${insert_client_script} ${server_host} ${host} ${i} ${no_thread} ${now_date}-${index_type} &"
	done
done

echo "start perf server on ${server_host}"

java $JVM_PARAM -cp ${cls_path} ${cls_name_insert_server} ${index_type} ${data_input_dir} ${stat_file_name} ${client_hosts_file} ${no_process} ${no_thread} ${workload_cls_name} ${workload_desc_path}
