#!/bin/bash
$SPARK_HOME/bin/spark-submit --class "utils.Day_Ita" --master "local" target/handson-spark-1.0.jar
