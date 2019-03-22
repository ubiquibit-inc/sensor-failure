package com.ubiquibit.buoy

import java.util.logging.Logger

import com.redis.RedisClient
import com.ubiquibit.Redis

/**
  * Weather station repository...
  */
trait StationRepository extends Serializable {

  def readStations(): Seq[StationInfo]

  def readStation(stationId: StationId): Option[StationInfo]

  def getImportStatus(stationId: StationId, buoyData: BuoyData): Option[ImportStatus]

  def updateImportStatus(stationId: StationId, buoyData: BuoyData, importStatus: ImportStatus): Option[ImportStatus]

  def deleteStations(): Boolean

  def saveStation(stationInfo: StationInfo): Option[StationId]

}

object StationRepository {

  private[buoy] val stationKeyPattern = "stationId:"

  private[buoy] def redisKey(stationId: StationId): String = {
    require(stationId.toString.length > 3)
    s"$stationKeyPattern$stationId"
  }

}

class StationRepositoryImpl(env: {
  val redis: Redis
}) extends StationRepository {

  val log = Logger.getLogger(getClass.getName)

  private val redis: RedisClient = env.redis.client

  // this is hacky, but I don't like the current serialization libraries for redis
  private val stationIdField = "stationId"
  private val freqField = "reportFrequencyMinutes"
  private val lastReportField = "lastReportUTC"
  private val stationKeyPattern = StationRepository.stationKeyPattern


  def updateImportStatus(stationId: StationId, buoyData: BuoyData, importStatus: ImportStatus): Option[ImportStatus] = {
    if (stationExists(stationId).isEmpty) return None
    require(BuoyData.values.contains(buoyData))
    importStatus match {
      case ERROR | WORKING | DONE =>
        if (redis.hmset(StationRepository.redisKey(stationId), Seq(buoyData.toString -> importStatus.toString))) Some(importStatus)
        else {
          log.severe(s"Error updating stationId $stationId to $importStatus.")
          Some(ERROR)
        }
      case _ => {
        log.severe(s"Unsupported import status: $importStatus")
        None
      }
    }
  }

  def getImportStatus(stationId: StationId, buoyData: BuoyData): Option[ImportStatus] = {
    val bdType = buoyData.toString.toUpperCase
    redis.hmget(StationRepository.redisKey(stationId), bdType)
      .flatMap(_.get(bdType))
      .flatMap(ImportStatus.valueOf)
  }

  def readStations(): Seq[StationInfo] = {
    val keys = redis.keys(s"$stationKeyPattern*")
      .map { k => k.filter(_.isDefined).map(_.get) }
    if (keys.isEmpty) return Seq()
    val strings: List[String] = keys.get.map(_.substring(stationKeyPattern.length))
    strings.map(StationId.makeStationId).flatMap(readStation)
  }

  def readStation(stationId: StationId): Option[StationInfo] = {
    val result = redis.hmget(StationRepository.redisKey(stationId), StationInfo.fields: _*)
    if (result.isDefined) {
      StationInfo.valueOf(result.get)
    }
    else None
  }

  def saveStation(stationInfo: StationInfo): Option[StationId] = {
    val stationId = stationInfo.stationId
    if (redis.hmset(StationRepository.redisKey(stationId), stationInfo.toMap)) Some(stationId)
    else None
  }

  def deleteStations(): Boolean = {
    val stations = readStations()
    val total = stations.size
    val deleted = stations.map(sId =>
      redis.del(StationRepository.redisKey(sId.stationId)) match {
        case Some(d) => d
        case _ => 0
      }
    ).sum
    val failures = total - deleted
    log.info(s"(Redis) DELETE >> Deleted $deleted of $total stations.")
    if (failures > 0) log.severe(s"(Redis) DELETE >> $failures errors occurred.")
    deleted > 0
  }

  // whether station is known to redis
  private def stationExists(stationId: StationId): Option[StationId] = {
    if (redis.keys(StationRepository.redisKey(stationId)).isDefined) Some(stationId)
    else None
  }

}
