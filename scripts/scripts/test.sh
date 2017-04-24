#!/bin/bash

fun_CheckConf(){
	if [ $# -le 1 ]; then
		echo "parameter $1 not found, exit"
		exit
	fi
}

fun_KillUniClient(){
	max=5
	for((i=0;i<${max};i++)) do
		for name in "UniServer UniClient"; do
        	pid=`jps | grep ${name} | cut -d ' ' -f 1`
        	if [ "${pid}x" != "x" ]; then
            	echo "kill ${pid} on ${host}"
            	kill -9 ${pid}
    		fi
		done
    done
    echo "kill hbase done"
}


fun_KillUniClient

