#!/bin/bash -x

# This script creates a kafka topic for each station (that produces a txt feed).

set KAFKA_HOME="/Users/jason/sbin/kafka_2.12-2.1.1"
set KAFKA_TOPICS_FILE="/tmp/stations.txt"

ls -l | grep "txt" | grep -v "ship_obs" | awk '{ print $9 }' | sed 's/\./ /' | awk '{ print $1 }' | sort -u >> ${KAFKA_TOPICS_FILE}
while read -r line; do ${KAFKA_HOME}/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $line; done < ${KAFKA_TOPICS_FILE}

