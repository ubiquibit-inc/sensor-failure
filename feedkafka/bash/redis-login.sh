#!/bin/bash -x

docker run -it --link redis:redis --rm redis redis-cli -h redis -p 6379