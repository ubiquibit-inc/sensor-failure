package com.ubiquibit.buoy

import scala.util.Try

/**
  * NDBC data is laid out in - let's call it a semantically-surprising way.
  * This file is for some dead reckoning on the data that was downloaded, as
  * described in the README.md file...
  */
object FileReckoning {

  import java.io.File

  val buoyDataDirectory = "/Users/jason/scratch/sensor-failure/data/www.ndbc.noaa.gov/data/realtime2/"

  /**
    * All filenames in the dataset.
    *
    * @param dirName dir to search
    * @param pred    predicate for filter
    * @return all such filenames (including extensions)
    */
  def filenames(dirName: String = buoyDataDirectory, pred: (File) => Boolean = (_) => true): List[String] = {

    val shipFileName = "ship_obs.txt"
    val skipShips: (File) => Boolean = (f: File) => !f.getName.equals(shipFileName)

    val file = new File(dirName)
    file.listFiles
      .filter(_.isFile)
      .filter(pred)
      .filter(skipShips)
      .map(_.getName)
      .toList
  }

  /**
    * @return distinct filenames (w/o extension, asciibetical order)
    */
  def distinctFilenames(): Seq[String] = {
    val alpha = filenames().sortWith((a, b) => a.compareTo(b) < 0)
    val x: List[String] = alpha.map {
      _.split("\\.")(0)
    }
    x.distinct
  }

  /**
    * Take a filename and derive a StationId from it (polymorphic...)
    *
    * @param str some string
    * @return such a beast
    */
  def makeStationId(str: String): StationId = {
    def tryBuoyId(str: String): Try[StationId] = {
      val s1 = str.substring(0, 2)
      val s2 = str.substring(2)
      Try(BuoyId(s1.toInt, s2))
    }

    tryBuoyId(str) getOrElse CmanId(str.substring(0, 2), str.substring(2))
  }

  /**
    * A good enough place for a list of station ids (calculated at time of this object's initialization...
    */
  val stationIds: Seq[StationId] = distinctFilenames().map(makeStationId)

}


