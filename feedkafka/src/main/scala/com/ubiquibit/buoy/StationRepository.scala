package com.ubiquibit.buoy

import com.redis.RedisClient
import com.ubiquibit.{Redis, TimeHelper}

import scala.collection.Map

/**
  * Weather station repository - online information about the stations in the system.
  */
trait StationRepository {

  /**
    * Saves a record for each station represented in the data directory **with default values**.
    */
  def initStations(): Unit

  def readStations(): Seq[StationInfo]

  /**
    * @param stationId    an existing station id
    * @param buoyData     any BuoyData type (except UNSUPPORTED)
    * @param importStatus either WORKING, DONE or ERROR
    */
  def updateImportStatus(stationId: StationId, buoyData: BuoyData, importStatus: ImportStatus): Option[ImportStatus]

  /**
    * @param stationId an existing stationid
    * @param buoyData  some feed for it
    * @return current status
    */
  def getImportStatus(stationId: StationId, buoyData: BuoyData): Option[ImportStatus]

  private[ubiquibit] def deleteStations(): Unit

}

class StationRepositorImpl(env: {
  val fileReckoning: FileReckoning
  val redis: Redis
}) extends StationRepository {

  private val redis: RedisClient = env.redis.client
  private val filez: FileReckoning = env.fileReckoning

  // this is hacky, but I don't like the current serialization libraries for redis
  private val stationIdField = "stationId"
  private val freqField = "reportFrequencyMinutes"
  private val lastReportField = "lastReportUTC"

  private def redisKey(stationId: StationId): String = {
    require(stationId.toString.length > 3)
    s"stationId:$stationId"
  }

  def updateImportStatus(stationId: StationId, buoyData: BuoyData, importStatus: ImportStatus): Option[ImportStatus] = {
    if (stationExists(stationId).isEmpty) return None
    require(BuoyData.values.contains(buoyData))
    importStatus match {
      case ERROR | WORKING | DONE =>
        if (redis.hmset(redisKey(stationId), Seq(buoyData.toString -> importStatus.toString))) Some(importStatus)
        else {
          println(s"Error updating stationId $stationId to $importStatus.")
          Some(ERROR)
        }
      case _ => {
        println(s"Unsupported import status: $importStatus")
        None
      }
    }
  }

  def getImportStatus(stationId: StationId, buoyData: BuoyData): Option[ImportStatus] = {
    val bdType = buoyData.toString.toUpperCase
    redis.hmget(redisKey(stationId), bdType)
      .flatMap(_.get(bdType))
      .flatMap(ImportStatus.valueOf)
  }

  def initStations(): Unit = {
    val total = filez.stationIds.length
    val successes: Int = filez.stationIds
      .map { s => redis.hmset(redisKey(s), valueOf(s.toString)) }
      .count(_ == true)
    val failures = total - successes
    println(s"(Redis) SAVE >> Initialized $successes of $total stations.")
    if (failures > 0) println(s"(Redis) SAVE >> $failures errors occurred")
  }

  val idToInfo: (StationId) => Option[StationInfo] = (staId: StationId) => {
    val rc = redisKey(staId)
    val hm: Option[Map[String, String]] = redis.hmget(rc, stationIdField, freqField, lastReportField)
    hm match {
      case Some(m: Map[String, String]) => for {
        ff <- m get freqField
        lrf <- m get lastReportField
        sif <- m get stationIdField
      } yield StationInfo(StationId.makeStationId(sif), ff.toInt, lrf)
      case _ => None
    }
  }

  def readStations(): Seq[StationInfo] = {
    val total = filez.stationIds.length
    val stationInfo = filez
      .stationIds
      .flatMap { id => idToInfo(id) }
    val errors = total - stationInfo.length
    println(s"(Redis) READ >> Read info for ${stationInfo.length}/$total stations.")
    if (errors > 0) println(s"(Redis) READ >> $errors errors occurred.")
    stationInfo
  }

  private[ubiquibit] def deleteStations(): Unit = {
    val total = filez.stationIds.length
    val deleted = filez.stationIds.map(sId =>
      redis.del(redisKey(sId)) match {
        case Some(d) => d
        case _ => 0
      }
    ).sum
    val failures = total - deleted
    println(s"(Redis) DELETE >> Deleted $deleted of $total stations.")
    if (failures > 0) println(s"(Redis) DELETE >> $failures errors occurred.")
  }

  private[ubiquibit] def valueOf(stationId: String, reportFrequencyMinutes: Int = 0): Map[String, String] = {
    val m: Map[String, String] = Map(stationIdField -> stationId,
      freqField -> reportFrequencyMinutes.toString,
      lastReportField -> TimeHelper.epochTimeZeroUTC().toString
    )
    val rdy = READY.toString.toUpperCase
    val l: Map[String, String] =
      filez
        .supportedTypes
        .map(_.ext.toUpperCase)
        .map(k => (k, rdy))
        .toMap
    m ++ l
  }

  // whether station is known to redis
  private def stationExists(stationId: StationId): Option[StationId] = {
    if (redis.hmget(redisKey(stationId)).isDefined) Some(stationId)
    else None
  }

}
