package com.ubiquibit.buoy

import StationId.makeStationId

/**
  * NDBC data is laid out in - let's call it a semantically-surprising way.
  * This file is for some dead reckoning on the data that was downloaded, as
  * described in the README.md file...
  */
object FileReckoning {

  import java.io.File

  val buoyDataDirectory = "/Users/jason/scratch/sensor-failure/data/www.ndbc.noaa.gov/data/realtime2/"

  val supportedTypes: Set[BuoyData] = Set(Text)
  val filenameSupported: (String) => Boolean = { absolutePath => supportedTypes.exists(_.same(new File(absolutePath))) }
  val fileSupported: (File) => Boolean = { f => supportedTypes.exists(_.same(f)) }

  /**
    * @return a list of files sitting on disk
    */
  def supportedFiles: List[File] = {
    filenames(fq = true)
      .filter(filenameSupported)
      .map(new File(_))
  }

  /**
    * A map from StationId to it's supported BuoyData outputs (on disk)
    */
  def supportByStation(): Map[StationId, Seq[BuoyData]] = {
    pairs().groupBy(_._1).mapValues(_.map(_._2))
  }

  // all station/output files on disk
  private def pairs(): List[(StationId, BuoyData)] = {
    supportedFiles
      .map(f => f.getName)
      .map(_.split("\\."))
      .map { arr => (makeStationId(arr(0)), BuoyData.values.find(_.ext.equalsIgnoreCase(arr(1))).getOrElse(Undefined)) }
      .filterNot(_._2 == Undefined) // hacky
  }

  // All filenames in the dataset.
  private def filenames(dirName: String = buoyDataDirectory, fq: Boolean = false, pred: (File) => Boolean = (_) => true): List[String] = {

    val shipFileName = "ship_obs.txt"
    val skipShips: (File) => Boolean = (f: File) => !f.getName.equals(shipFileName)

    val file = new File(dirName)
    val names = file.listFiles
      .filter(_.isFile)
      .filter(pred)
      .filter(skipShips)
      .map(_.getName)

    if (fq) names.map(fn => s"$buoyDataDirectory$fn").toList
    else names.toList
  }

  // distinct filenames (w/o extension, in asciibetical order)
  private def distinctFilenames(): Seq[String] = {
    val alpha = filenames().sortWith((a, b) => a.compareTo(b) < 0)
    val x: List[String] = alpha.map {
      _.split("\\.")(0)
    }
    x.distinct
  }

  /**
    * A good enough place for a list of station ids (calculated at time of this object's initialization...
    */
  val stationIds: Seq[StationId] = distinctFilenames().map(makeStationId)

//  def main(args: Array[String]): Unit = {
//    supportByStation().foreach(println)
//  }

}
