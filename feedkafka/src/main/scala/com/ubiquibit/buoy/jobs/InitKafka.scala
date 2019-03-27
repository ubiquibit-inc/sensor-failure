package com.ubiquibit.buoy.jobs

import java.util.logging.Logger

import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit.buoy._
import com.ubiquibit.buoy.parse.TextParser
import com.ubiquibit.buoy.serialize.DefSer
import com.ubiquibit.{RandomElements, Spark, TopicNamer, Wiring}
import org.apache.spark.sql.SparkSession

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
}) extends InitKafka with TopicNamer with RandomElements with Serializable {

  private val log: Logger = Logger.getLogger(getClass.getName)

  private val repo: StationRepository = env.stationRepository
  private val filez: FileReckoning = env.fileReckoning
  private val conf: Config = ConfigFactory.load()

  implicit val spark: SparkSession = env.spark.session

  def hasFeedReady(si: WxStation): Boolean = {
    si.feeds.exists(_._2 == READY)
  }

  def run(): Unit = {

    val candidates: Seq[(StationId, BuoyData)] =
      repo
        .readStations()
        .filter(hasFeedReady)
        .map(sta => (sta.stationId, sta.feeds))
        .map { case ((record: (StationId, Map[BuoyData, ImportStatus]))) =>
          val first = record._2.filter((m) => m._2 == READY).take(1).head
          (record._1, first._1)
        }

    log.info(s"Found ${candidates.size} candidate stations.")

    randomElemOf(candidates).foreach(t => { // there's really (at most?) one

      val stationId = t._1
      val buoyData = t._2
      log.fine(s"Proceeding with $stationId's $buoyData file.")

      repo.updateImportStatus(stationId, buoyData, WORKING)

      import spark.implicits._

      log.info(s"Processing $stationId's $buoyData feed.")
      val file = filez.getFile(stationId, buoyData)
      val parser = new TextParser
      val ds = parser
        .parseFile(file.get.getAbsolutePath)
        .as[TextRecord]

      val topic: String = topicName(stationId, buoyData)
      log.info(s"Will attempt to sink to Kafka topic = $topic.")

      val df = ds.map(_.valueOf())
      df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", conf.getString("bootstrap.servers"))
        .option("topic", topic)
        .save()

      val cnt = df.count()

      val someDF = Seq(("1", "jason"), ("2", "shelley")).toDF("key", "value")

      log.info(s"Processed $cnt lines of $stationId's $buoyData feed.")

      repo.updateImportStatus(stationId, buoyData, DONE)

    })

  }

}

object InitKafkaImpl {

  def main(args: Array[String]): Unit = {
    Wiring.initKafka.run()
  }

}