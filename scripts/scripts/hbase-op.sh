#!/bin/bash

hbase_nodes="`pwd`/../conf/hbase-nodes"
clients="`pwd`/../conf/clients"

jps_names_hbase="HQuorumPeer HRegionServer HMaster DITBClient DITBServer HybridClient HybridServer"
jps_names_hdfs="SecondaryNameNode DataNode NameNode"
jps_names_client="DITBClient DITBServer PerfClient HybridClient HybridServer"

usr=`whoami`
data_dir="/home/${usr}/data/"
#data_dir="/data/chfeng/data/"
HDP_VERSION=2.5.1
data_dir_hadoop="${data_dir}/hadoop-data/hadoop-${HDP_VERSION}"
data_dir_hadoop_name="${data_dir_hadoop}/name"
data_dir_hadoop_data="${data_dir_hadoop}/data"
data_dir_hadoop_tmp="${data_dir_hadoop}/tmp"
data_dir_hadoop_log="${HADOOP_HOME}/logs"
HBASE_VERSION=1.2.1
data_dir_hbase_tmp="${data_dir}/hbase-data/hbase-${HBASE_VERSION}-tmp"
data_dir_hbase_lcindex="${data_dir}/hbase-data/lcindex"
data_dir_hbase_log="${HBASE_HOME}/logs"
data_dir_zk="${data_dir}/zk-data"

fun_usage(){
	echo "use the following cmd:"
	echo "	c / clean: clean hbase and hdfs filesm, re-format hdfs"
	echo "	k / kill: kill hbase and hdfs"
	echo "	conf: copy configurations from current node" 
	echo "	jar: copy client, server and common jars from current node"
	echo "	jps: jps"
	echo "	redep: kill all, copy conf and start all"
}

fun_RemoveDirIfExists(){
	if [ $# -lt 2 ]; then
		echo "need at least two parameter"
	else
		host=$1
		path=$2
		echo "remove dir ${path} on ${host}"
		ssh ${host} "rm -rf ${path}"
	fi
}

fun_ClearHDFS(){
    for host in `cat ${hbase_nodes}`; do
 	    fun_RemoveDirIfExists ${host} ${data_dir_hadoop_data} 
 	    fun_RemoveDirIfExists ${host} ${data_dir_hadoop_name} 
 	    fun_RemoveDirIfExists ${host} ${data_dir_hadoop_tmp} 
 	    fun_RemoveDirIfExists ${host} ${data_dir_hadoop_log} 
		fun_RemoveDirIfExists ${host} /tmp/hadoop-`whoami`*
	done
	hadoop namenode -format -force
}

fun_ClearHBaseTmp(){
    for host in `cat ${hbase_nodes}`; do
 	    fun_RemoveDirIfExists ${host} ${data_dir_hbase_tmp} 
 	    fun_RemoveDirIfExists ${host} ${data_dir_hbase_lcindex} 
 	    fun_RemoveDirIfExists ${host} ${data_dir_hbase_log} 
		fun_RemoveDirIfExists ${host} /tmp/hbase-`whoami`*
		fun_RemoveDirIfExists ${host} ${data_dir_zk}
	done
}

fun_ClearLog(){
    for host in `cat ${hbase_nodes}`; do
 	    fun_RemoveDirIfExists ${host} ${data_dir_hadoop_log}
		fun_RemoveDirIfExists ${host} ${data_dir_hbase_log}
	done
}

fun_KillHBase(){
	for host in `cat ${hbase_nodes}`; do 
		for name in `echo ${jps_names_hbase}`; do
			pid=`ssh ${host} "jps | grep ${name} | cut -d ' ' -f 1"`
			if [ "${pid}x" != "x" ]; then
				echo "kill ${pid} on ${host}"
				ssh ${host} "kill -9 ${pid}"
            fi
        done
    done
    echo "kill hbase done"
}

fun_KillClients(){
	for host in `cat ${clients}`; do 
		for name in `echo ${jps_names_client}`; do
			pid=`ssh ${host} "jps | grep ${name} | cut -d ' ' -f 1"`
			while [ "${pid}x" != "x" ]; do
				echo "kill ${pid} on ${host}"
				ssh ${host} "kill -9 ${pid}"
				pid=`ssh ${host} "jps | grep ${name} | cut -d ' ' -f 1"`
    		done
        done
    done
    echo "kill clients done"
}

fun_KillHDFS(){
	for host in `cat ${hbase_nodes}`; do 
		for name in `echo ${jps_names_hdfs}`; do
            pid=`ssh ${host} "jps | grep ${name} | cut -d ' ' -f 1"`
            if [ "${pid}x" != "x" ]; then
                echo "kill ${pid} on ${host}"
                ssh ${host} "kill -9 ${pid}"
            fi
        done
    done
    echo "kill hdfs done"
}

# copy conf files
fun_Conf(){
	for host in `cat ${hbase_nodes}`; do
		fun_ScpToRemote ${HBASE_HOME}/conf ${HBASE_HOME} ${host}
		fun_ScpToRemote ${HADOOP_HOME}/etc ${HADOOP_HOME} ${host}
	done
	for host in `cat ${clients}`; do
		fun_ScpToRemote ${HBASE_HOME}/conf ${HBASE_HOME} ${host}
		fun_ScpToRemote ${HADOOP_HOME}/etc ${HADOOP_HOME} ${host}
	done
}

# copy jar files, only client.jar server.jar common.jar
fun_Jar(){
	lib_dir="${HBASE_HOME}/lib"
	for host in `cat ${hbase_nodes}`; do
		for f in client server common; do
			fun_ScpToRemote ${lib_dir}/hbase-${f}-${HBASE_VERSION}.jar ${lib_dir} $host
		done
	done
	for host in `cat ${clients}`; do
		for f in client server common; do
			fun_ScpToRemote ${lib_dir}/hbase-${f}-${HBASE_VERSION}.jar ${lib_dir} $host
		done
	done
}

# run jps
fun_Jps(){
	for host in `cat ${hbase_nodes}`; do
		ssh ${host} jps
	done
}

# copy log files
fun_CopyHDFSLog(){
	for host in `cat ${hbase_nodes}`; do
		fun_ScpFromRemote ${data_dir_hadoop_log} $1 ${host}
	done
}

# copy log files
fun_CopyHBaseLog(){
	for host in `cat ${hbase_nodes}`; do
		fun_ScpFromRemote ${data_dir_hbase_log} $1 ${host}
	done
}

# basic scp function
fun_ScpToRemote(){
	if [ $# -ne 3 ]; then
		echo "need 3 parameters in scp"
		exit
	fi
	src=$1
	dest=$2
	host=$3
	echo "scp ${src} to ${dest} on ${host}"
	scp -rq ${src} ${host}:${dest}
}

# basic scp function
fun_ScpFromRemote(){
	if [ $# -ne 3 ]; then
		echo "need 3 parameters in scp, now see $@"
		exit	
	fi
	src=$1
	dest=$2
	host=$3
	echo "scp ${src} at ${dest} to ${host}"
	scp -rq ${host}:${src} ${dest}
}

if [ $# = 0 ]; then
	fun_usage
	exit
elif [ ! -e "${hbase_nodes}" ]; then
    echo "hbase node file not exist: ${hbase_nodes}"
    exit
fi

cmd=$1

if [ "${cmd}x" = "kx" ] || [ "${cmd}x" = "killx" ]; then
	echo "clean all and reboot"
	fun_KillClients
    fun_KillHBase
    fun_KillHDFS
elif [ "${cmd}x" = "cx" ] || [ "${cmd}x" = "cleanx" ]; then
	echo "clean hbase and hdfs"
    fun_ClearHDFS
    fun_ClearHBaseTmp
elif [ "${cmd}x" = "clearlogx" ]; then
	echo "clear log"
	fun_ClearLog
elif [ "${cmd}x" = "copyhdfsx" ]; then
	echo "copy hdfs logs to local"
	fun_CopyHDFSLog $2
elif [ "${cmd}x" = "copyhbasex" ]; then
	echo "copy hbase logs to local"
	fun_CopyHBaseLog $2
elif [ "${cmd}x" = "confx" ]; then
	echo "copy configurations"
	fun_Conf
elif [ "${cmd}x" = "jarx" ]; then
	echo "copy jars"
	fun_Jar
elif [ "${cmd}x" = "hbx" ]; then
	fun_KillClients
    fun_KillHBase
	hdfs dfs -rm -r /hbase
    fun_ClearHBaseTmp
elif [ "${cmd}x" = "kcx" ]; then
	fun_KillClients
    fun_KillHBase
    fun_KillHDFS
    fun_ClearHDFS
    fun_ClearHBaseTmp
elif [ "${cmd}x" = "kcsx" ]; then
	fun_KillClients
    fun_KillHBase
    fun_KillHDFS
    fun_ClearHDFS
    fun_ClearHBaseTmp
	start-dfs.sh && sleep 10 && start-hbase.sh
elif [ "${cmd}x" = "startx" ]; then
	start-dfs.sh && sleep 10 && start-hbase.sh
elif [ "${cmd}x" = "jpsx" ]; then
	fun_Jps
elif [ "${cmd}x" = "redepx" ]; then
	fun_KillClients
    fun_KillHBase
    fun_KillHDFS
    fun_ClearHDFS
    fun_ClearHBaseTmp
	fun_Conf
	start-dfs.sh && sleep 10 && start-hbase.sh
else
	echo "unknow cmd ${cmd}, please follow the usage"
	fun_usage
fi
