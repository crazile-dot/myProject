#!/bin/bash
$SPARK_HOME/bin/spark-submit --class "Query1SparkSQL" --master "local" target/handson-spark-1.0.jar
