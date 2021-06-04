#!/bin/bash
echo "Starting Spark in Debug Mode..."
rm -r ../debug_results
$SPARK_HOME/bin/spark-submit --class "queries.QueryMain" --master "local" ../target/SABD_Project1-0.0.1-SNAPSHOT-jar-with-dependencies.jar true