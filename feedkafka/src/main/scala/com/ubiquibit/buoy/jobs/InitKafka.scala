package com.ubiquibit.buoy.jobs

import com.ubiquibit.buoy._
import com.ubiquibit.{Spark, TimeHelper}
import com.ubiquibit.buoy.parse.TextParser

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

  private val repo: StationRepository = env.stationRepository
  private val filez: FileReckoning = env.fileReckoning
  private val rand = scala.util.Random

  implicit val spark: Spark = env.spark

  def hasFeedReady(si: StationInfo): Boolean = {
    si.feeds.exists(_._2 == READY)
  }

  def randomElemOf[T](seq: Seq[T]): Option[T] = {
    if (seq.isEmpty) None
    else seq lift rand.nextInt(seq.length)
  }

  def run(): Unit = {

    TimeHelper.randomNap()

    // ^^^ so that everybody doesn't grab the same file
    // this is by no means a guarantee, but is good enough
    // for now

    val candidates: Seq[(StationId, BuoyData)] =
      repo
        .readStations()
        .filter(hasFeedReady)
        .map(sta => (sta.stationId, sta.feeds))
        .map { case ((record: (StationId, Map[BuoyData, ImportStatus]))) =>
          val first = record._2.filter((m) => m._2 == READY).take(1).head
          (record._1, first._1)
        }

    randomElemOf(candidates).foreach(t => { // there's really only one

      val stationId = t._1
      val buoyData = t._2

      repo.updateImportStatus(stationId, buoyData, WORKING)

      println(s"Processing $stationId's $buoyData feed.")
      val file = filez.getFile(stationId, buoyData)
      val parser = new TextParser
      val df = parser.parse(file.get.getAbsolutePath)
      val cnt = df.count()

      println(s"Processed $cnt lines of $stationId's $buoyData feed.")

      repo.updateImportStatus(stationId, buoyData, DONE)
    })

  }

}
