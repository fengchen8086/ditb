#!/bin/bash

#mvn dependency:go-offline eclipse:eclipse -DskipTests
#mvn dependency:go-offline -DskipTests
#mvn package -Pdist -DskipTests -Dtar

hdfs_jar_path=share/hadoop/hdfs/hadoop-hdfs-2.5.2.jar

fun_Usage(){
	echo "Usage: mvn-work.sh <command>"
	echo "  eclipse      build eclipse environment"
	echo "  clean        execute 'mvn clean'"
	echo "  build        build some?"
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
elif  [ "${cmd}x" = "buildx" ]; then
	echo "want to build"
	mvn dependency:go-offline package -DskipTests -Dtar -e
elif  [ "${cmd}x" = "unknownx" ]; then
	echo "unknown"
else
	echo "unknown cmd '${cmd}', see usage"
	fun_Usage
	exit
fi

exit

if [ "$1x" = "skipx" ] || [ "$1x" = "sx" ]; then
	echo "skip hadoop-hdfs compile"
else
	echo "compile hadoop-hdfs"
	mvn dependency:go-offline package -Pdist -DskipTests -Dtar -e
#	mvn dependency:go-offline package -Pdist,native -DskipTests -Dtar -e
fi

# cp -r /home/winter/workspace/eclipse/hadoop-3.0.0-trunk-151203/hadoop-dist/target/hadoop-3.0.0-SNAPSHOT/share/hadoop ${HADOOP_HOME}/share/

#cp hadoop-dist/target/hadoop-2.5.2/${hdfs_jar_path} ${HADOOP_HOME}/${hdfs_jar_path}
#cp hadoop-dist/target/hadoop-2.5.2/${hdfs_jar_path} ${HBASE_HOME}/lib

# ref http://stackoverflow.com/questions/7233328/how-do-i-configure-maven-for-offline-development
