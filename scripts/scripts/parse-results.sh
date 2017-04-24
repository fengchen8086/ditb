#!/bin/bash
if [ $# -ne 2 ]; then
	echo "need time as prefix"
	exit
fi 

now=$1
run_type=$2
output_dir=`pwd`/../output

res_file=${output_dir}/res-${run_type}-${now}
echo "" > ${res_file}
for dir in `ls ${output_dir} | grep ${now}`; do 
	log_dir=${output_dir}/${dir}
	if [ -d "${log_dir}" ]; then
		index_type=`echo ${dir} | tail -c 8`
		cat ${log_dir}/insert.log | grep "latency report" | sed "s/^/${index_type}/g" | sed "s/$/insert/g" >> ${res_file}
		cat ${log_dir}/insert.log | grep "report put" | sed "s/^/${index_type}/g" | sed "s/$/put-time/g" >> ${res_file}
		cat ${log_dir}/scan.log | grep "report scan" | sed "s/^/${index_type}/g" | sed "s/$/scan/g" >> ${res_file}
		cat ${log_dir}/storage |  sed "s/^/${index_type}/g" | sed "s/$/storage/g" >> ${res_file}
		cat ${log_dir}/dstat-insert/dstat/* | grep -v "\"" | sed -e /^$/d | awk -F',' '{print $14}' | sed "s/^/${index_type}/g" | sed "s/$/insert-dstat/g" >> ${res_file}
		cat ${log_dir}/dstat-scan/dstat/* | grep -v "\"" | sed -e /^$/d | awk -F',' '{print $14}' | sed "s/^/${index_type}/g" | sed "s/$/scan-dstat/g" >> ${res_file}
		cat ${log_dir}/remote-log/* | grep "summary" | head -n 5 >> ${res_file}
	fi
done
