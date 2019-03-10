package com.ubiquibit

import java.time.LocalDateTime

import com.ubiquibit.buoy.StationId
import com.ubiquibit.TimeHelper._

import scala.collection.Map

/**
  * A repository for weather station metadata.
  */
trait StationRepository extends Redis {

  import com.ubiquibit.buoy.FileReckoning._

  // this is a little hacky, but I don't like the redis serialization libraries that are currently on offer
  case class StationInfo(stationId: String, reportFrequencyMinutes: String, lastReport: Any = epochTimeZeroUTC())

  private val stationIdField = "stationId"
  private val freqField = "reportFrequencyMinutes"
  private val lastReportField = "lastReportUTC"

  private def redisKey(stationId: StationId): String = {
    require(stationId.toString.length > 3)
    s"stationId:$stationId"
  }

  private def valueOf(stationId: String, reportFrequencyMinutes: Int = 0, lastReport: LocalDateTime = epochTimeZeroUTC()): Map[String, Any] = {
    Map(stationIdField -> stationId, freqField -> reportFrequencyMinutes, lastReportField -> lastReport.toInstant(TimeHelper.defaultOffset))
  }

  /**
    * Saves a record for each station represented in the data directory **with default values**.
    */
  def saveStations(): Unit = {
    val total = stationIds.length
    val successes: Int = stationIds
      .map { s => redis.hmset(redisKey(s), valueOf(s.toString)) }
      .count(_ == true)
    val failures = total - successes
    println(s"(Redis) SAVE >> Initialized $successes of $total stations.")
    if (failures > 0) println(s"(Redis) SAVE >> $failures errors occurred")
  }

  private [ubiquibit] def deleteStations(): Unit = {
    val total = stationIds.length
    val deleted = stationIds.map(sId =>
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
    val total = stationIds.length
    val stations = stationIds.map{ sId =>
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
