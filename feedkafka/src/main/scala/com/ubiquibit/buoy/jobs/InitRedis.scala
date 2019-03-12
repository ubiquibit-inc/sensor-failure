package com.ubiquibit.buoy.jobs

import com.ubiquibit.StationRepository

/**
  * We use Redis to store a little bit of information about the system. After
  * downloading data a local data directory, this program will scan it and
  * create some meta-data information in Redis.
  */
object InitRedis extends InitRedis {

  def main(args: Array[String]): Unit = {
    doRun()
  }

}

class InitRedis extends StationRepository {

  def doRun() = {
    println(s"${stationIds.length} stations detected on disk.")

    var stations = readStations()

    if( stations.isEmpty ){
      println("Redis has no station information, creating...")
      initStations()
      stations = readStations()
    }

    if( stations.length != stationIds.length ){
      println(s"Mismatch between ${stations.length} stations in Redis and ${stationIds.length} on disk, wiping Redis and starting over.")
      deleteStations()
      initStations()
    }

    println("Exiting.")

  }
}
