#!/bin/bash

#mvn dependency:go-offline eclipse:eclipse -DskipTests
#mvn dependency:go-offline -DskipTests
#mvn package -Pdist -DskipTests -Dtar

fun_Usage(){
	echo "Usage: mvn-work.sh <command>"
	echo "  eclipse      build eclipse environment"
	echo "  clean        execute 'mvn clean'"
	echo "  build | b	 build"
	echo "  scp | s		 scp jars to lingcloud"
    echo "  unknown      unknown"
}

if [ $# = 0 ]; then
	fun_Usage
	exit
fi

cmd=$1
shift

if [ "${cmd}x" = "eclipsex" ]; then
	echo "building eclipse environment"
	mvn clean install -DskipTests
	mvn eclipse:eclipse
elif  [ "${cmd}x" = "cleanx" ]; then
	echo "want to clean"
	mvn clean
elif  [ "${cmd}x" = "buildx" ] || [ "${cmd}x" = "bx" ]; then
	echo "want to build"
	mvn dependency:go-offline package -DskipTests -Dtar -e
elif  [ "${cmd}x" = "cpx" ]; then
#	hbase_lib_dir=/media/winter/E
	hbase_lib_dir=/home/winter/softwares/hbase-1.2.1/lib
	echo "copy client and server jar to local hbase lib ${hbase_lib_dir}"
	cp hbase-client/target/hbase-client-1.2.1.jar ${hbase_lib_dir}
	cp hbase-server/target/hbase-server-1.2.1.jar ${hbase_lib_dir}
	cp hbase-common/target/hbase-common-1.2.1.jar ${hbase_lib_dir}
elif  [ "${cmd}x" = "scpx" ] || [ "${cmd}x" = "sx" ]; then
	echo "scp client and server jar to remote side"
	#scp hbase-client/target/hbase-client-1.2.1.jar 
	#scp hbase-server/target/hbase-server-1.2.1.jar 
	#scp hbase-common/target/hbase-common-1.2.1.jar 
else
	echo "unknown cmd '${cmd}', see usage"
	fun_Usage
fi
