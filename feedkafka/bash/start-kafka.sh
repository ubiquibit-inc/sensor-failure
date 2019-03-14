#!/bin/bash -x

# Note: this script works "only approximately"

echo "this script works only approximately"

readonly KAFKA_HOME="/Users/jason/sbin/kafka_2.12-2.1.1"
readonly ZK_HOSTS="localhost:2181"

# ${KAFKA_HOME}/bin/zookeeper-server-start.sh ${KAFKA_HOME}/config/zookeeper.properties > /dev/null 2>&1 &
# sleep 5
# ${KAFKA_HOME}/bin/kafka-server-start.sh config/server.properties > /dev/null 2>&1 &

# ${KAFKA_HOME}/bin/kafka-topics.sh --create --zookeeper "${ZK_HOSTS}" --replication-factor 1 --partitions 1 --topic test
# ${KAFKA_HOME}/bin/kafka-topics.sh --list --zookeeper "${ZK_HOSTS}"


