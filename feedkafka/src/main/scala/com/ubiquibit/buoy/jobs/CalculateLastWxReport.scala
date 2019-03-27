package com.ubiquibit.buoy.jobs

import java.util.logging.Logger

import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit._
import com.ubiquibit.buoy._
import org.apache.spark.SparkContext
import org.apache.spark.sql._

/**
  * This fun littel job reads up the static data files and computes the most
  * recent [[WxStation]] report. And no one even cares...
  *
  * @param env DI injected in [[Wiring]]
  */
class CalculateLastWxReport(env: {
    val spark: Spark
}) extends RandomElements with TopicNamer with QueryNamer with Deserializer {

  @transient private val Log: Logger = Logger.getLogger(getClass.getName)

  val ss: SparkSession = env.spark.session
  val sc: SparkContext = env.spark.sc

  private val conf: Config = ConfigFactory.load()

  def run(): Unit = {

      val stationId = StationId.makeStationId("62149")
      val feedType = Text

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

      Log.info(s"Last $feedType report received from $stationId at $ts}. (Redis updated.)")
      println(s"Last $feedType report received from $stationId at $ts}. (Redis updated.)")

  }

}

object CalculateLastWxReport {
  def main(args: Array[String]): Unit = {
    Wiring.updateLastWxReport.run()
  }
}
