/*
 * Copyright (c) 2019.
 *
 * This work, (c) by jason@ubiquibit.com
 *
 * This work is licensed under a
 * Creative Commons Attribution-ShareAlike 4.0 International License.
 *
 * You should have received a copy of the license along with this
 * work.  If not, see <http://creativecommons.org/licenses/by-sa/4.0/>.
 *
 */

package com.ubiquibit

import com.ubiquibit.buoy.jobs._
import com.ubiquibit.buoy.jobs.setup._
import com.ubiquibit.buoy.jobs.util.{StageFeeds, WriteInterruptRecord}
import com.ubiquibit.buoy.{FileReckoning, FileReckoningImpl, StationRepository, StationRepositoryImpl}

/**
  * As described here: http://jonasboner.com/real-world-scala-dependency-injection-di/
  * in "other alternative #1"
  */
object Wiring {

  private val hahaScala: Unit = {
    spark.sc.setLogLevel("ERROR")
  }

  lazy val redis: Redis = new RedisImpl
  lazy val spark: Spark = new SparkImpl

  lazy val fileReckoning: FileReckoning = new FileReckoningImpl
  lazy val stationRepository: StationRepository = new StationRepositoryImpl(this)

  lazy val initKafka: InitKafka = new InitKafkaImpl(this)
  lazy val initRedis: InitRedis = new InitRedisImpl(this)
  lazy val updateLastWxReport: CalculateLastWxReport = new CalculateLastWxReport(this)
  lazy val stageFromRedis: StageFeeds = new StageFeeds(this)
  lazy val wxStream: WxStream = new WxStream(this)

  lazy val writeInterruptRecord: WriteInterruptRecord = new WriteInterruptRecord(this)

}
