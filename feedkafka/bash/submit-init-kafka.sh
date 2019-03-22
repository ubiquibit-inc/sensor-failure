#!/bin/bash

./bin/spark-submit --class "com.ubiquibit.buoy.jobs.InitKafkaImpl" --master "spark://Flob.local:7077" --deploy-mode cluster --executor-cores 4 --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0" "/Users/jason/scratch/sensor-failure/feedkafka/target/scala-2.11/feedkafka-assembly-1.0.jar"