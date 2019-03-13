package com.ubiquibit.buoy

import java.io.File

import StationId.makeStationId
import com.typesafe.config.{Config, ConfigFactory}

/**
  * NDBC data is laid out in - let's call it a semantically-surprising way.
  * This file is for some dead reckoning on the data that was downloaded, as
  * described in the README.md file...
  */
trait FileReckoning {

  /**
    * A list of station ids - note: stations are not reported if they don't have a supported data feed
    */
  def stationIds: Seq[StationId]

  /**
    * A map from StationId to it's supported BuoyData outputs (on disk)
    */
  def supportByStation: Map[StationId, Seq[BuoyData]]

  def getFile(stationId: StationId, ofType: BuoyData): Option[File]

  def supportedTypes: List[BuoyData] = List(Text) // TODO move to its own trait

}

class FileReckoningImpl extends FileReckoning{

  import java.io.File

  private val config: Config = ConfigFactory.load()
  private [buoy] val buoyData = s"${config.getString("bouy.data.subdir")}"
  private val buoyDataDirectory = s"${config.getString("data.directory")}$buoyData"

  override val supportedTypes: List[BuoyData] = List(Text)
  private val filenameSupported: (String) => Boolean = { absolutePath => supportedTypes.exists(_.same(new File(absolutePath))) }

  def getFile(stationId: StationId, ofType: BuoyData): Option[File] = {
    if( !supportByStation().exists(_._1 == stationId) ) None
    val expectedName = s"${stationId.toString}.${ofType.ext}".toUpperCase
    supportedFiles.find{ _.getName.equalsIgnoreCase(expectedName) }
  }

  def stationIds: Seq[StationId] = pairs().map(_._1).distinct

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

  // @return a list of files sitting on disk
  private def supportedFiles: List[File] = {
    filenames(fq = true)
      .filter(filenameSupported)
      .map(new File(_))
  }

}
