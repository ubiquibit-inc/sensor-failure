package com.ubiquibit.buoy.jobs

import com.ubiquibit.buoy._
import com.ubiquibit.{Spark, TimeHelper}
import com.ubiquibit.buoy.parse.TextParser

import scala.util.Random

/**
  * This util is for bootstrapping the system, and comes after Redis
  * has been initialized. It's a one-off operation, it:
  *
  * 1. Asks Redis for a file that is in the READY state
  * 2. Reads it up as a DataFrame
  * 3. Pumps it into Kafka
  * 4. Exits
  *
  * Subsequent writes to Kafka will be through another mechanism.
  */
trait InitKafka {

  def run(): Unit

}

class InitKafkaImpl(env: {
  val stationRepository: StationRepository
  val spark: Spark
  val fileReckoning: FileReckoning
}) extends InitKafka with Serializable {

  val repo: StationRepository = env.stationRepository
  val filez: FileReckoning = env.fileReckoning

  implicit val spark: Spark = env.spark

  def hasFeedReady(si: StationInfo): Boolean = {
    si.feeds.exists(_._2 == READY)
  }

  private val rand = scala.util.Random

  def randomElemOf[T](seq: Seq[T]): Option[T] = {
    if (seq.isEmpty) None
    else seq lift rand.nextInt(seq.length)
  }

  def run(): Unit = {

    TimeHelper.randomNap() // so that everybody doesn't grab the same file

    val poss: Seq[(StationId, BuoyData)] =
      repo
        .readStations()
        .filter(_.feeds.exists(_._2 == READY))
        .map(sta => (sta.stationId, sta.feeds))
        .map { case ((record: (StationId, Map[BuoyData, ImportStatus]))) =>
          val first = record._2.filter((m) => m._2 == READY).take(1).head
          (record._1, first._1)
        }

    randomElemOf(poss).flatMap(t => {
      val stationId = t._1
      val buoyData = t._2
      repo.updateImportStatus(stationId, buoyData, WORKING)
      val file = filez.getFile(stationId, buoyData)
      val parser = new TextParser
      val df = parser.parse(file.get.getAbsolutePath)
      repo.updateImportStatus(stationId, buoyData, DONE)
    })

  }

}