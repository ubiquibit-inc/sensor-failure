#!/bin/bash -x

# This script creates a kafka topic for each station (that produces a txt feed).

readonly DATA_DIR="/Users/jason/scratch/sensor-failure/data/www.ndbc.noaa.gov/data/realtime2"
readonly KAFKA_HOME="/Users/jason/sbin/kafka_2.12-2.1.1"
readonly KAFKA_TOPICS_FILE="/tmp/stations.txt"

rm -f "${KAFKA_TOPICS_FILE}"
ls -l ${DATA_DIR} | grep "txt" | grep -v "ship_obs" | awk '{ print $9 }' | sed 's/\./ /' | awk '{ print $1 }' | sort -u >"${KAFKA_TOPICS_FILE}"
while read -r line; do ${KAFKA_HOME}/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $line"-TXT"; done <"${KAFKA_TOPICS_FILE}"

