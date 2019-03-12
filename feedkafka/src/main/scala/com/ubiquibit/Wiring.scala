package com.ubiquibit

import com.ubiquibit.buoy.jobs.{InitKafka, InitKafkaImpl, InitRedis, InitRedisImpl}
import com.ubiquibit.buoy.{FileReckoning, FileReckoningImpl}

object Wiring {

  lazy val fileReckoning: FileReckoning = new FileReckoningImpl()
  lazy val redis: Redis = new RedisImpl
  lazy val stationRepository: StationRepository = new StationRepositorImpl(this)
  lazy val spark: Spark = new SparkImpl
  lazy val initKafka: InitKafka = new InitKafkaImpl(this)
  lazy val initRedis: InitRedis = new InitRedisImpl(this)

}
