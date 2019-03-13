package com.ubiquibit.buoy.jobs

import com.ubiquibit.buoy.{FileReckoning, StationRepository}

/**
  * Interprets what's on disk and turns it into reference data in Redis.
  *
  * Another process is responsible for keeping the system up-to-date: This
  * proc is intended as an administrative (bootstrap) one-off...
  *
  */
trait InitRedis {

  /**
    * Interpret what's on disk and create weather station metadata in Redis.
    */
  def run(): Unit

}

class InitRedisImpl(env: {
  val fileReckoning: FileReckoning
  val stationRepository: StationRepository
}) extends InitRedis {

  private val filez: FileReckoning = env.fileReckoning
  private val repo: StationRepository = env.stationRepository

  def run(): Unit = {
    println(s"${filez.stationIds.length} stations detected on disk.")

    val stationIds = filez.stationIds
    var stations = repo.readStations()

    if (stations.isEmpty) {
      println("Redis has no station information, creating...")
      repo.initStations()
      stations = repo.readStations()
    }

    if (stations.length != stationIds.length) {
      println(s"Mismatch between ${stations.length} stations in Redis and ${stationIds.length} on disk, wiping Redis and starting over.")
      repo.deleteStations()
      repo.initStations()
    }

    println("Exiting.")

  }

}
