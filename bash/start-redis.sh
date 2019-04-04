#!/bin/bash -x
docker run -d --name redis -p 6379:6379  -v /Users/jason/scratch/sensor-failure/docker/redis.conf:/redis.conf redis redis-server /redis.conf
