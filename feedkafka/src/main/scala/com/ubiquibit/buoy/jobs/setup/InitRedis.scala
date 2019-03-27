package com.ubiquibit.buoy.jobs.setup

import com.ubiquibit.buoy._

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

    val stationIds = filez.stationIds
    println(s"${stationIds.length} stations detected on disk.")

    val stations = repo.readStations()
    println(s"${stations.length} stations are in Redis.")

    if (stations.isEmpty) {
      println("Redis has no station information, creating...")
      val savedCount = saveStations()
      println(s"Saved $savedCount stations to Redis.")
    }

    if (repo.readStations().length != stationIds.length) {
      println(s"Mismatch between ${stations.length} stations in Redis and ${stationIds.length} on disk, wiping Redis and starting over.")
      repo.deleteStations()
      val savedCount = saveStations()
      println(s"Saved $savedCount stations to Redis.")
    }

    println("Exiting.")

  }

  def saveStations(): Int = {
    val successes = for (st <- filez.stationInfo())
      yield repo.saveStation(st)
    successes.count(_.isDefined)
  }

}