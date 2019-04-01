# sensor-failure

This project demonstrates how to use Spark's Arbitrary Stateful Streaming Engine to generate ML data from a publicly-available sensor network.

You too can have a fully-operational buoy anomaly detector running on your workstation. Just follow along. 

#### Background

[NOAA](https://www.noaa.gov/) operates a world-wide network of weather stations under auspices of the [National Data Buoy Center](https://www.ndbc.noaa.gov/). 

Reporting weather stations have multiple configurations. Sensor arrays are either land-based (C-man), moored, or floating. Weather reports are transmitted via [GOES](https://en.wikipedia.org/wiki/Geostationary_Operational_Environmental_Satellite) or [Iridium](https://en.wikipedia.org/wiki/Iridium_satellite_constellation) satellite networks to a ground facility in Wallops Island, Virginia.

Stations are equipped with an array of near-surface-level and underwater sensors. In other words: Different stations report different data.

Stations report data on a fixed period or continuously. Data feeds are aggregated hourly and published to a publicly-accessible web-share location.

From [whence](https://www.ndbc.noaa.gov/data/realtime2/) we start.   

#### Quickstart

#####  download buoy data 

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

##### Kafka & Redis

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

# wait...
```

- run [InitKafkaImpl](src/main/scala/com/ubiquibit/buoy/jobs/setup/InitKafka.scala) 

> It loads one (text) data feed per run from filesystem - recommend 3-4 runs for simple setup.

```bash

# before

redis:6379> hmget "stationId:46082" "TXT"
1) "DOWNLOADED"

# Loads hard-coded Station 46082...

./bin/spark-submit --class "com.ubiquibit.buoy.jobs.setup.InitKafkaImpl" --master "spark://Flob.local:7077" --deploy-mode cluster --executor-cores 4 --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0" "/Users/jason/scratch/sensor-failure/target/scala-2.11/sensorfailure-assembly-1.0.jar"

# OR loads a station of your choosing...

./bin/spark-submit --class "com.ubiquibit.buoy.jobs.setup.InitKafkaImpl" --master "spark://Flob.local:7077" --deploy-mode cluster --executor-cores 4 --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0" "/Users/jason/scratch/sensor-failure/Target/scala-2.11/sensorfailure-assembly-1.0.jar" "BZST2"

# verify by checking in Redis:

redis:6379> hmget "stationId:46082" "TXT"
1) "KAFKALOADED"
```

##### Init WxStream

- run [StageFeeds](src/main/scala/com/ubiquibit/buoy/jobs/util/StageFeeds.scala)

> It writes a file to the staging directory that will later be used by [WxStream](src/main/scala/com/ubiquibit/buoy/jobs/WxStream.scala)

##### Run WxStream

```bash
/bin/spark-submit --class "com.ubiquibit.buoy.jobs.WxStream" --master "spark://Flob.local:7077" --deploy-mode cluster --executor-cores 4 --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0" "/Users/jason/scratch/sensor-failure/target/scala-2.11/feedkafka-assembly-1.0.jar"
``` 

> check the driver's stdout log and [SparkUI](http://localhost:8080)

Note: WxStream console output shows up in the *driver* stdout. StationInterrupt and other debug logging shows up in the *application* stderr (if configured in `$SPARK_HOME/conf/log4.properties`)
