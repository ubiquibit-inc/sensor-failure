package com.ubiquibit

import java.time.LocalDateTime

import com.ubiquibit.buoy._
import com.ubiquibit.TimeHelper._

import scala.collection.Map

/**
  * A repository for weather station metadata.
  */
trait StationRepository extends Redis {

  import com.ubiquibit.buoy.{FileReckoning => FR}

  // this is hacky, but I don't like the current serialization libraries for redis
  private val stationIdField = "stationId"
  private val freqField = "reportFrequencyMinutes"
  private val lastReportField = "lastReportUTC"
  private val textFileStateField = "lastReportUTC"

  private def redisKey(stationId: StationId): String = {
    require(stationId.toString.length > 3)
    s"stationId:$stationId"
  }

  private def valueOf(stationId: String, reportFrequencyMinutes: Int = 0, lastReport: LocalDateTime = epochTimeZeroUTC(), textState: ImportState = UNSUPPORTED): Map[String, Any] = {
    Map(stationIdField -> stationId,
      freqField -> reportFrequencyMinutes,
      lastReportField -> lastReport.toInstant(TimeHelper.defaultOffset),
      textFileStateField -> textState
    )
  }

  /**
    * Saves a record for each station represented in the data directory **with default values**.
    */
  def saveStations(): Unit = {
    val total = FR.stationIds.length
    val successes: Int = FR.stationIds
      .map { s => redis.hmset(redisKey(s), valueOf(s.toString)) }
      .count(_ == true)
    val failures = total - successes
    println(s"(Redis) SAVE >> Initialized $successes of $total stations.")
    if (failures > 0) println(s"(Redis) SAVE >> $failures errors occurred")
  }

  private def initialImportStatusFromFiles(): Unit = {
    val fileTypes = FR.supportedTypes
//    val FR.stationIds.filter()
    ???
  }

  private [ubiquibit] def deleteStations(): Unit = {
    val total = FR.stationIds.length
    val deleted = FR.stationIds.map(sId =>
      redis.del(redisKey(sId)) match {
        case Some(d) => d
        case _ => 0
      }
    ).sum
    val failures = total - deleted
    println(s"(Redis) DELETE >> Deleted $deleted of $total stations.")
    if (failures > 0) println(s"(Redis) DELETE >> $failures errors occurred.")
  }

  def readStations(): Seq[Any] = {
    val total = FR.stationIds.length
    val stations = FR.stationIds.map{ sId =>
      redis.hmget(redisKey(sId), stationIdField, freqField, lastReportField) match {
        case s: Some[scala.collection.immutable.Map[String, String]] =>
          val m = s.get
          StationInfo(m(stationIdField), m(freqField), m(lastReportField))
        case _ =>
      }
    }
    val errors = total - stations.length
    println(s"(Redis) READ >> Read info for ${stations.length}/$total stations.")
    if( errors > 0 ) println(s"(Redis) READ >> $errors errors occurred.")
    stations
  }

}
