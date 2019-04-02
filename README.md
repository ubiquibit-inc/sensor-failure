# sensor-failure

*anomaly detection in large sensor networks*

We demonstrate real-time processing of streaming sensor data with persistence for machine learning.

![basic flow](img/buoy-flow.png) 

The system implements Spark Arbitrary Stateful Processing to consume Kafka feeds. Upon detection of interrupt, a trailing event stream is persisted in Redis for ML analyses.

#### Background

[NOAA](https://www.noaa.gov/) manages a world-wide network of weather stations under auspices of the [National Data Buoy Center](https://www.ndbc.noaa.gov/). 

Stations are land-based, moored, and floating.

![land based](img/sbio1.png)
![moored](img/image003.png)
![floating](img/12m1.png)

Stations supply near-surface and underwater sensor arrays and report at fixed intervals or continuously. 

Data are transmitted via [GOES](https://en.wikipedia.org/wiki/Geostationary_Operational_Environmental_Satellite) or [Iridium](https://en.wikipedia.org/wiki/Iridium_satellite_constellation) satellite networks to a ground facility in Wallops Island, Virginia. Feeds are aggregated hourly and published to a publicly-accessible web-share location.

![sensor data flow](img/NDBC-dataflow.png)

Where possible, NDBC supplements with feeds from assets outside of its direct jurisdiction, including ship observations.

#### Results

The following trace shows sensor interrupts from weather station NPXN6: As the Stream is processed, the second to the last sensor sends a signal and then goes silent again.

![interrupts](img/two-interrupts.png)

Each time any monitored station stops sending a signal, a block of 16 records is processed by our streaming sinks.

Spark supports a wide-variety of output sinks. The depicted example uses a simple [ForeachWriter](src/main/scala/com/ubiquibit/buoy/jobs/InterruptWriter.scala) that writes to disk.

For longer-term persistance, we write the output to Redis, where we can pick it up later from Machine Learning jobs.

#### Nuts & Bolts

##### WxStream

The heart of the implementation takes place in the WxStream Spark Application. 

Each WxStream consumes from [a number of station topics](src/main/scala/com/ubiquibit/buoy/jobs/WxStream.scala#L71) then uses [flatMapGroupWithState](src/main/scala/com/ubiquibit/buoy/jobs/WxStream.scala#L83) to *splay out* execution to the cluster.

StationInterrupts' [updateInterrupts function](src/main/scala/com/ubiquibit/buoy/jobs/StationInterrupts.scala#L44) calculates interrupts and "onlineAgain" for each consecutive set of records.

![core processing](img/processed.png)

Together with Spark's arbitrary stateful processing engine, the algorithm is capable of handling out-of-order event arrivals, record timeouts (to preserve memory), and scales out to support arbitrarily large sensor networks.

##### Input

NDBC's realtime [web-share](https://www.ndbc.noaa.gov/data/realtime2/) is updated hourly - "usually by 15 minutes after the hour". Data files store a 45 (check!) day history for a given station and are updated at the *top* of the file. 

For example:

![topoffile](img/head.png)

Two runtime options are provided for queuing up data for `WxStream`: `LiveKafkaFeeder` and `InitKafkaImpl` 

`InitKafkaImpl` is a Spark application that processes fully-downloaded data files and writes them to Kafka. It needs to be run one time per station. 

If you want to do retrospective analysis for a few stations, this is a good option. 

`LiveKafkaFeeder` is a JVM application that checks for changes to the files on the web-share. As changes are detected, it will write them to Kafka.

This is a good option if you want to run `WxStream` for extended periods of time.

You may use either one, but it's probably best to start with InitKafkaImpl even in the case that you want to run the live feeder.

##### Output

TODO 

#### Howto

#####  download buoy data 

```bash 
# from within `BASEDIR`...
% mkdir data && cd data ;
% wget -r -np -R "index.html*" https://www.ndbc.noaa.gov/data/realtime2/ ; 

# wait...

```

We have data from ~950 WxStations reporting ~17 different output formats. 

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

##### Pre-requisites

- Apache Kafka (2.12-2.1.1 or similar)
- Apache Spark (2.4.0-bin-hadoop-2.7 or similar)
- sbt (1.2.7 or similar)
- scala (2.11.12 or similar)
- Redis (run via Docker - below)

##### Clone & Build

```bash

% git clone https://github.com/jasonnerothin/sensor-failure.git
% cd sensor-failure 
```
- edit [application.properties](src/main/resources/application.properties)
```bash
% sbt assembly

...

[info] Packaging /Users/jason/scratch/sensor-failure/target/scala-2.11/sensorfailure-assembly-1.0.jar ...
[info] Done packaging.
[success] Total time: 31 s, completed Apr 1, 2019 4:05:45 PM
[IJ]sbt:sensorfailure> 

```

##### Kafka & Redis

- Run Redis (on Docker)
````bash
# FROM `BASEDIR`...
cd bash
./start-redis.sh
````
- run [InitRedisImpl](src/main/scala/com/ubiquibit/buoy/jobs/setup/InitKafka.scala) 

- Install and start Kafka 

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

./redis-login.sh

...

redis:6379> hmget "stationId:46082" "TXT"
1) "DOWNLOADED"

# Loads hard-coded Station 46082...

${SPARK_HOME}/bin/spark-submit --class "com.ubiquibit.buoy.jobs.setup.InitKafkaImpl" --master "spark://${SPARK_HOST}:7077" --deploy-mode cluster --executor-cores 2 --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0" "/Users/jason/scratch/sensor-failure/target/scala-2.11/sensorfailure-assembly-1.0.jar"
19/04/02 09:06:23 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable

# OR loads a station of your choosing...

${SPARK_HOME}/bin/spark-submit --class "com.ubiquibit.buoy.jobs.setup.InitKafkaImpl" --master "spark://${SPARK_HOST}:7077" --deploy-mode cluster --executor-cores 2 --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0" "/Users/jason/scratch/sensor-failure/Target/scala-2.11/sensorfailure-assembly-1.0.jar" "BZST2"

# after

./redis-login.sh

...

redis:6379> hmget "stationId:46082" "TXT"
1) "KAFKALOADED"
```

##### Init WxStream

- run [StageFeeds](src/main/scala/com/ubiquibit/buoy/jobs/util/StageFeeds.scala)

> It writes a file to the staging directory that will later be used by [WxStream](src/main/scala/com/ubiquibit/buoy/jobs/WxStream.scala)

##### Run WxStream

```bash
${SPARK_HOME}/bin/spark-submit --class "com.ubiquibit.buoy.jobs.WxStream" --master "spark://${SPARK_HOST}:7077" --deploy-mode cluster --executor-cores 4 --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0" "/Users/jason/scratch/sensor-failure/target/scala-2.11/sensorfailure-assembly-1.0.jar"
``` 

> check the driver's stdout log and [SparkUI](http://localhost:8080)

Note: WxStream console output shows up in the *driver* stdout. StationInterrupt and other debug logging shows up in the *application* stderr (if configured in `$SPARK_HOME/conf/log4.properties`)
