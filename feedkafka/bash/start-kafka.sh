#!/bin/bash -x

# Note: this script works "only approximately"

readonly KAFKA_HOME="/Users/jason/sbin/kafka_2.12-2.1.1"
readonly ZK_HOSTS="Flob.local:2181"

${KAFKA_HOME}/bin/zookeeper-server-start.sh -daemon ${KAFKA_HOME}/config/zookeeper.properties
echo "waiting for ZK to fire up"
sleep 10
${KAFKA_HOME}/bin/kafka-server-start.sh -daemon config/server.properties

# ${KAFKA_HOME}/bin/kafka-topics.sh --create --zookeeper "${ZK_HOSTS}" --replication-factor 1 --partitions 1 --topic test
# ${KAFKA_HOME}/bin/kafka-topics.sh --list --zookeeper "${ZK_HOSTS}"
