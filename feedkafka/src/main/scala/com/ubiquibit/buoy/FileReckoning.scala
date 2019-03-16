package com.ubiquibit.buoy

import java.io.File

import StationId.makeStationId
import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit.TimeHelper

import scala.collection.Map

/**
  * Info about NDBC real-time data (stored on the filesystem)
  * is served by this interface
  */
trait FileReckoning {

  def stationIds(): Seq[StationId]

  // ALL feeds
  def feeds(): Map[StationId, Seq[BuoyData]]

  // only SUPPORTED feeds are included
  def stationInfo(): Seq[StationInfo]

  // only SUPPORTED files
  def getFile(stationId: StationId, feed: BuoyData): Option[File]

  // only SUPPORTED files
  def pairs(): List[(StationId, BuoyData)]

}

trait SupportedFeeds {

  def supported: List[BuoyData] = List(Text) // TODO load from Config instead, perhaps dynamically

}

class FileReckoningImpl extends FileReckoning with SupportedFeeds {

  import java.io.File

  private val config: Config = ConfigFactory.load()
  private[buoy] val buoyData = s"${config.getString("buoy.data.subdir")}"
  private val buoyDataDirectory = s"${config.getString("data.directory")}$buoyData"

  private val filenameSupported: (String) => Boolean = { absolutePath => supported.exists(_.same(new File(absolutePath))) }

  def getFile(stationId: StationId, ofType: BuoyData): Option[File] = {
    if (!feeds.exists(_._1 == stationId)) None
    val expectedName = s"${stationId.toString}.${ofType.ext}".toUpperCase
    supportedFiles.find {
      _.getName.equalsIgnoreCase(expectedName)
    }
  }

  def stationIds(): Seq[StationId] = pairs().map(_._1).distinct

  def feeds(): Map[StationId, Seq[BuoyData]] = {
    pairs().groupBy(_._1).mapValues(_.map(_._2))
  }

  def stationInfo(): Seq[StationInfo] = {
    pairs()
      .groupBy(_._1)
      .map { (t) =>
        val staId = t._1
        val feeds = t._2
        (staId, feeds.filter(_._1 == staId).map(_._2 -> READY).toMap)
        }.map(u => StationInfo(u._1, 0, TimeHelper.epochTimeZeroUTC().toString, u._2))
      .toSeq
  }

  // all station/output files on disk
  def pairs(): List[(StationId, BuoyData)] = {
    supportedFiles
      .map(f => f.getName)
      .map(_.split("\\."))
      .map { arr => (makeStationId(arr(0)), BuoyData.values.find(_.ext.equalsIgnoreCase(arr(1)))) }
      .filter(_._2.isDefined)
      .map((a) => (a._1, a._2.get))
  }

  // @return a list of files sitting on disk
  private def supportedFiles: List[File] = {
    filenames(fq = true)
      .filter(filenameSupported)
      .map(new File(_))
  }

  // All filenames in the data set.
  private def filenames(dirName: String = buoyDataDirectory, fq: Boolean = false, p: (File) => Boolean = (_) => true): List[String] = {

    val shipFileName = "ship_obs.txt"
    val skipShips: (File) => Boolean = (f: File) => !f.getName.equals(shipFileName)

    val file = new File(dirName)
    val names = file.listFiles
      .filter(_.isFile)
      .filter(p)
      .filter(skipShips)
      .map(_.getName)

    if (fq) names.map(fn => s"$buoyDataDirectory$fn").toList
    else names.toList
  }

}
