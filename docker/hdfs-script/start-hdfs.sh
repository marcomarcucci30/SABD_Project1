#!/bin/bash
hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh
hdfs dfs -mkdir /data
hdfs dfs -chmod -R 777 /data
hdfs dfs -mkdir /output
hdfs dfs -chmod -R 777 /output