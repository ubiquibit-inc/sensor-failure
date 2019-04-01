#!/bin/bash -x

readonly SPARK_HOME="/Users/jason/sbin/spark-2.4.0-bin-hadoop2.7"
readonly SPARK_MASTER_URL="spark://Flob.local:7077

. ${SPARK_HOME}/sbin/start-master.sh
sleep 3
. ${SPARK_HOME}/sbin/start-slave.sh ${SPARK_MASTER_URL}
sleep 1