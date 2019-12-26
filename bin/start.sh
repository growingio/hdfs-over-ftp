#!/usr/bin/env bash

if [ -n "$(jps -m | grep HdfsOverFtpServer)" ]
then
  echo "it's running, stop it first."
  exit 1
fi

base="$(cd "`dirname "$0"`/.."; pwd)"

HADOOP_CONF_CLASSPATH=$HADOOP_CONF_DIR
if [[ -n "$HADOOP_HOME" ]]; then
  HADOOP_CONF_CLASSPATH=$HADOOP_HOME/etc/conf:$HADOOP_CONF_CLASSPATH
fi

class_path=$base/conf:$base/lib/*:$HADOOP_CONF_CLASSPATH
vm_opts="-Xms1G -Xmx2G"

start_cmd="java -cp $class_path $vm_opts org.apache.hadoop.contrib.ftp.HdfsOverFtpServer"
echo ${start_cmd}
exec ${start_cmd}
