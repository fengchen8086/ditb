#!/bin/bash
if [ $# -ne 1 ]; then
    echo "need file path from parameter"
fi

echo "note use 2>/dev/null to skip hadoop warnings"

fun_ShowFile(){
        for file in `hdfs dfs -ls $1 | awk '{print $8}'`; do
                if [ $file != $1 ]; then
                        replica=`hdfs fsck ${file} | grep "Average block" | awk '{print $4}'`
                        echo "${file} has ${replica} replicas"
                        if [ `echo "${replica} >= 2.0" | bc` -eq 1 ]; then
                                fun_ShowFile $file
                        fi
                fi
        done
}

fun_ShowFile $@
