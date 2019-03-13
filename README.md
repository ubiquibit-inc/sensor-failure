# sensor-failure

Detect and classify sensor failures.

####  Download buoy data from NOAA

```bash 
% mkdir data && cd data ;
% wget -r -np -R "index.html*" https://www.ndbc.noaa.gov/data/realtime2/ ; 

# wait...

% ls -l | awk '{ print $9 }' | sed 's/\./ /' | sort -u | awk '{ print $2 }' | sort -u ; 

adcp
adcp2
cwind
dart
data_spec
drift
hkp
ocean
rain
spec
srad
supl
swdir
swdir2
swr1
swr2
txt

% ls -l | awk '{ print $9 }' | sed 's/\./ /' | sort -u | awk '{ print $1 }' | sort -u | wc -l ;
     
951

```

So we have data from 950 buoys reporting 17 different formats...

Running the BuoyFinder worksheet, yields the following:

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

#### Start Kafka & Redis

````bash
./docker/start-redis.sh
````
Install apache Kafka (choose version compatible with Scala 2.12) and start.
```bash 
# edit, then run...
./feedkafka/bash/create-kafka-topics

```



