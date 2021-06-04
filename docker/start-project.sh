#!/bin/bash
docker network create --driver bridge apache_network
docker run -t -i --rm -h hbase-docker -p 2181:2181 -p 8080:8080 -p 8085:8085 -p 9090:9090 -p 9095:9095 -p 16000:16000 -p 16010:16010 -p 16020:16020 -p 16201:16201 -p 16301:16301 -d --network=apache_network --name=hbase-docker harisekhon/hbase:1.4
docker run -t -i --rm -h slave1-docker -p 9864:9864 -d --network=apache_network --name=slave1 effeerre/hadoop
docker run -t -i --rm -h slave2-docker -p 9863:9864 -d --network=apache_network --name=slave2 effeerre/hadoop
docker run -t -i --rm -h slave3-docker -p 9862:9864 -d --network=apache_network --name=slave3 effeerre/hadoop
docker run -t -i --rm -h master-docker -p 9870:9870 -p 9871:54310 -d --network=apache_network --name=master effeerre/hadoop
docker cp hdfs-script/start-hdfs.sh master:/start-hdfs.sh
docker exec -it master sh /start-hdfs.sh
docker run -p 9880:9880 -d --network=apache_network -v nifi_conf:/opt/nifi/nifi-current/conf -v nifi_state:/opt/nifi/nifi-current/state -e NIFI_WEB_HTTP_PORT='9880' --name=nifi apache/nifi
docker cp nifi-hdfs nifi:/opt/nifi/nifi-current
var=0
while [ $var -ne 4 ]; do
	echo $var "files avalaible ..."
	var=0
	docker exec -it master hdfs dfs -test -e /data/totale-popolazione.parquet
	if [ $? -eq 0 ]; then
		var=$((var + 1))
	fi
	sleep 1
	docker exec -it master hdfs dfs -test -e /data/punti-somministrazione-tipologia.parquet
	if [ $? -eq 0 ]; then
		var=$((var + 1))
	fi
	sleep 1
	docker exec -it master hdfs dfs -test -e /data/somministrazioni-vaccini-latest.parquet
	if [ $? -eq 0 ]; then
		var=$((var + 1))
	fi
	sleep 1
	docker exec -it master hdfs dfs -test -e /data/somministrazioni-vaccini-summary-latest.parquet
	if [ $? -eq 0 ]; then
		var=$((var + 1))
	fi
	sleep 2
done
echo "Environment ready!"
echo "Starting Spark ..."
$SPARK_HOME/bin/spark-submit --class "queries.QueryMain" --master "local" ../target/SABD_Project1-0.0.1-SNAPSHOT-jar-with-dependencies.jar false
