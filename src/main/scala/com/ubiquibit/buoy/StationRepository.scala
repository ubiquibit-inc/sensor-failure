package com.ubiquibit.buoy

import java.sql.Timestamp
import java.util.logging.Logger

import com.redis.RedisClient
import com.ubiquibit.{Redis, TimeHelper}

/**
  * Weather station repository...
  */
trait StationRepository extends Serializable {
  def updateLastReport(stationId: StationId, time: Timestamp): Unit

  def readStations(): Seq[WxStation]

  def readStation(stationId: StationId): Option[WxStation]

  def getImportStatus(stationId: StationId, buoyData: WxFeed): Option[WxFeedStatus]

  def updateImportStatus(stationId: StationId, buoyData: WxFeed, importStatus: WxFeedStatus): Option[WxFeedStatus]

  def deleteStations(): Boolean

  def saveStation(stationInfo: WxStation): Option[StationId]

}

object StationRepository extends Serializable{

  private[buoy] val stationKeyPattern = "stationId:"

  private[buoy] def redisKey(stationId: StationId): String = {
    require(stationId.toString.length > 3)
    s"$stationKeyPattern$stationId"
  }

}

class StationRepositoryImpl(env: {
  val redis: Redis
}) extends StationRepository {

  @transient private val Log = Logger.getLogger(getClass.getName)

  private val redis: RedisClient = env.redis.client

  // this is hacky, but I don't like the current serialization libraries for redis
  private val stationIdField = "stationId"
  private val freqField = "reportFrequencyMinutes"
  private val lastReportField = "lastReportUTC"
  private val stationKeyPattern = StationRepository.stationKeyPattern

  override def updateImportStatus(stationId: StationId, buoyData: WxFeed, importStatus: WxFeedStatus): Option[WxFeedStatus] = {
    if (stationExists(stationId).isEmpty) return None
    require(WxFeed.values.contains(buoyData))
    importStatus match {
      case ERROR | KAFKALOADING | KAFKALOADED =>
        if (redis.hmset(StationRepository.redisKey(stationId), Seq(buoyData.toString -> importStatus.toString))) Some(importStatus)
        else {
          Log.info(s"Error updating stationId $stationId to $importStatus.")
          Some(ERROR)
        }
      case _ => {
        Log.info(s"Unsupported import status: $importStatus")
        None
      }
    }
  }

  override def getImportStatus(stationId: StationId, buoyData: WxFeed): Option[WxFeedStatus] = {
    val bdType = buoyData.toString.toUpperCase
    redis.hmget(StationRepository.redisKey(stationId), bdType)
      .flatMap(_.get(bdType))
      .flatMap(WxFeedStatus.valueOf)
  }

  override def readStations(): Seq[WxStation] = {
    val keys = redis.keys(s"$stationKeyPattern*")
      .map { k => k.filter(_.isDefined).map(_.get) }
    if (keys.isEmpty) return Seq()
    val strings: List[String] = keys.get.map(_.substring(stationKeyPattern.length))
    Log.fine(s"Read out ${strings.size} StationIds from Redis.")
    strings.map(StationId.makeStationId).flatMap(readStation)
  }

  override def readStation(stationId: StationId): Option[WxStation] = {
    val result = redis.hmget(StationRepository.redisKey(stationId), WxStation.fields: _*)
    if (result.isDefined) {
      WxStation.valueOf(result.get)
    }
    else None
  }

  override def saveStation(stationInfo: WxStation): Option[StationId] = {
    val stationId = stationInfo.stationId
    if (redis.hmset(StationRepository.redisKey(stationId), stationInfo.toMap)) Some(stationId)
    else None
  }

  override def deleteStations(): Boolean = {
    val stations = readStations()
    val total = stations.size
    val deleted = stations.map(sId =>
      redis.del(StationRepository.redisKey(sId.stationId)) match {
        case Some(d) => d
        case _ => 0
      }
    ).sum
    val failures = total - deleted
    Log.info(s"(Redis) DELETE >> Deleted $deleted of $total stations.")
    if (failures > 0) Log.info(s"(Redis) DELETE >> $failures errors occurred.")
    deleted > 0
  }

  // whether station is known to redis
  private def stationExists(stationId: StationId): Option[StationId] = {
    if (redis.keys(StationRepository.redisKey(stationId)).isDefined) Some(stationId)
    else None
  }

  override def updateLastReport(stationId: StationId, time: Timestamp): Unit = {
    redis.hmset(StationRepository.redisKey(stationId), Seq(lastReportField -> time.toString))
  }

}
