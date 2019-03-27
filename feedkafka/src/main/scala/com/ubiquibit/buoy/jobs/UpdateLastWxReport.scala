package com.ubiquibit.buoy.jobs

import java.util.logging.Logger

import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit._
import com.ubiquibit.buoy._
import org.apache.spark.SparkContext
import org.apache.spark.sql._

/**
  * Reads up the static data files and computes the most recent [[WxStation]] report, then saves it in Redis.
  *
  * @param env DI injected in [[Wiring]]
  */
class UpdateLastWxReport(env: {
  val stationRepository: StationRepository
  val spark: Spark
}) extends RandomElements with TopicNamer with QueryNamer with Deserializer {

  @transient private val Log: Logger = Logger.getLogger(getClass.getName)

  val repo: StationRepository = env.stationRepository
  val ss: SparkSession = env.spark.session
  val sc: SparkContext = env.spark.sc

  private val conf: Config = ConfigFactory.load()

  def run(): Unit = {

      val stationId = StationId.makeStationId("62149") //result.get._1)
      val feedType = Text
//      val stationId = StationId.makeStationId(result.get._1)
//      val feedType = result.get._2.get._1

      val topic: String = topicName(stationId, feedType)

      Log.info(s"Calculating last $feedType report from $stationId.")
      println(s"Calculating last $feedType report from $stationId.")

      import ss.implicits._
      import org.apache.spark.sql.functions._

      val enc: Encoder[TextRecord] = Encoders.product[TextRecord]

      val staticSource = ss.read
        .format("kafka")
        .option("kafka.bootstrap.servers", conf.getString("bootstrap.servers"))
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("spark.sql.shuffle.partitions", conf.getString("spark.partitions"))
        .load()

      val lastReport = staticSource
        .map(deserialize)
        .withColumn("ts_long", unix_timestamp($"eventTime"))
        .sort($"ts_long".desc)
        .drop($"ts_long")
        .as[TextRecord]
        .take(1)
        .head

      val ts = lastReport.eventTime

      repo.updateLastReport(stationId, ts)

      Log.info(s"Last $feedType report received from $stationId at $ts}. (Redis updated.)")
      println(s"Last $feedType report received from $stationId at $ts}. (Redis updated.)")


//    else Log.info("No stations active at this time.")


  }

}

object UpdateLastWxReport {
  def main(args: Array[String]): Unit = {
    Wiring.updateLastWxReport.run()
  }
}
