package com.ubiquibit

object StationRepositorySpec extends StationRepository{

  def main(args: Array[String]): Unit = {
    deleteStations()
    saveStations()
    for (info <- readStations()) {
      println(info)
    }
  }

}
