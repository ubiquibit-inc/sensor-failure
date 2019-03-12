package com.ubiquibit

import java.time.LocalDateTime

import com.ubiquibit.buoy._
import com.ubiquibit.TimeHelper._

import scala.collection.Map

/**
  * A repository for weather station metadata.
  */
trait StationRepository extends FileReckoning with Redis {

  // this is hacky, but I don't like the current serialization libraries for redis
  private val stationIdField = "stationId"
  private val freqField = "reportFrequencyMinutes"
  private val lastReportField = "lastReportUTC"

  private def redisKey(stationId: StationId): String = {
    require(stationId.toString.length > 3)
    s"stationId:$stationId"
  }

  private[ubiquibit] def valueOf(stationId: String, reportFrequencyMinutes: Int = 0, lastReport: LocalDateTime = epochTimeZeroUTC()): Map[String, Any] = {
    val m = Map(stationIdField -> stationId,
      freqField -> reportFrequencyMinutes,
      lastReportField -> lastReport.toInstant(TimeHelper.defaultOffset)
    )
    val zv = READY.toString.toUpperCase
    val l = supportedTypes
      .map(_.ext.toUpperCase)
      .zipAll(zv, zv, zv)
    m ++ l
  }

  /**
    * @param stationId    an existing station id
    * @param buoyData     any BuoyData type (except UNSUPPORTED)
    * @param importStatus either WORKING, DONE or ERROR
    */
  def updateImportStatus(stationId: StationId, buoyData: BuoyData, importStatus: ImportStatus): Option[StationId] = {
    require(stationExists(stationId).isDefined)
    require(BuoyData.values.contains(buoyData))
    importStatus match {
      case ERROR | WORKING | DONE =>
        if (redis.hmset(stationId.toString, Seq(buoyData.toString -> importStatus.toString))) Some(stationId)
        else {
          println(s"Error updating stationId $stationId to $importStatus.")
          None
        }
      case _ => {
        println(s"Unsupported import status: $importStatus")
        None
      }
    }
  }

  // whether station is known to redis
  private def stationExists(stationId: StationId): Option[StationId] = {
    if (redis.hmget(redisKey(stationId)).isDefined) Some(stationId)
    else None
  }

  /**
    * @param stationId an existing stationid
    * @param buoyData  some feed for it
    * @return current status
    */
  def getImportStatus(stationId: StationId, buoyData: BuoyData): Option[ImportStatus] = {
    val status = redis.hmget(stationId.toString, buoyData.toString)
    if (status.isDefined) ImportStatus.valueOf(status.get(buoyData.toString))
    else None
  }

  /**
    * Saves a record for each station represented in the data directory **with default values**.
    */
  def initStations(): Unit = {
    val total = stationIds.length
    val successes: Int = stationIds
      .map { s => redis.hmset(redisKey(s), valueOf(s.toString)) }
      .count(_ == true)
    val failures = total - successes
    println(s"(Redis) SAVE >> Initialized $successes of $total stations.")
    if (failures > 0) println(s"(Redis) SAVE >> $failures errors occurred")
  }

  private[ubiquibit] def deleteStations(): Unit = {
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
    val stations = stationIds.map { sId =>
      redis.hmget(redisKey(sId), stationIdField, freqField, lastReportField) match {
        case s: Some[scala.collection.immutable.Map[String, String]] =>
          val m = s.get
          StationInfo(m(stationIdField), m(freqField), m(lastReportField))
        case _ =>
      }
    }
    val errors = total - stations.length
    println(s"(Redis) READ >> Read info for ${stations.length}/$total stations.")
    if (errors > 0) println(s"(Redis) READ >> $errors errors occurred.")
    stations
  }

}
