#!/bin/bash

# parameters for assigning input and output dir
script_root_dir="`pwd`/../"
additional_conf="${script_root_dir}/conf/winter-hbase.conf"
hbase_nodes="${script_root_dir}/conf/hbase-nodes"
client_hosts_file="${script_root_dir}/conf/clients"

for host in `cat ${client_hosts_file}`; do
    ssh ${host} "rm -rf ${script_root_dir}/output/remote-log/*"
done

rm -rf ${script_root_dir}/output/remote-log/*
