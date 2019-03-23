package com.ubiquibit

import com.ubiquibit.buoy.jobs._
import com.ubiquibit.buoy.{FileReckoning, FileReckoningImpl, StationRepository, StationRepositoryImpl}

/**
  * As described here: http://jonasboner.com/real-world-scala-dependency-injection-di/
  * in "other alternative #1"
  */
object Wiring {

  lazy val redis: Redis = new RedisImpl
  lazy val spark: Spark = new SparkImpl

  lazy val fileReckoning: FileReckoning = new FileReckoningImpl
  lazy val stationRepository: StationRepository = new StationRepositoryImpl(this)

  lazy val initKafka: InitKafka = new InitKafkaImpl(this)
  lazy val initRedis: InitRedis = new InitRedisImpl(this)
  lazy val reportFreqFromBeginning: ReportFreqFromBeginning = new ReportFreqFromBeginning(this)

}
