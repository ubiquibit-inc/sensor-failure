# sensor-failure

Detect and classify sensor failures in NOAA's NDBC network.

####  download buoy data 

```bash 
% mkdir data && cd data ;
% wget -r -np -R "index.html*" https://www.ndbc.noaa.gov/data/realtime2/ ; 

# wait...

```

We have data from 950 WxStations reporting 17 different formats. 

```bash
# Adcp files 29
# Adcp2 files 39
# Cwind files 75
# Dart files 45
# DataSpec files 152
# Drift files 8
# Hkp files 2
# Ocean files 178
# Rain files 36
# Spec files 312
# Srad files 17
# Supl files 65
# Swdir files 149
# Swr1 files 149
# Swr2 files 149
# Text files 799
```

#### Kafka & Redis

- edit [application.properties](src/main/resources/application.properties)
- Run Redis (on Docker)
````bash
cd bash
./start-redis.sh
````
- run [InitRedisImpl](src/main/scala/com/ubiquibit/buoy/jobs/setup/InitKafka.scala) 

- Install and start Kafka (version-compatible with Scala 2.11)

```bash 
# edit, then run...
./start-kafka.sh
./create-kafka-topics.sh
```

- run [InitKafkaImpl](src/main/scala/com/ubiquibit/buoy/jobs/setup/InitKafka.scala) 

> Load one data feed per run from filesystem - recommend 3-4 runs for simple setup.

Keep track of the Station ID that the App chooses by looking at stdout

#### Init WxStream

- run [StageFeedsFromRedis](src/main/scala/com/ubiquibit/buoy/jobs/setup/StageFeedsFromRedis.scala)

> Writes a file to the staging directory 

   


