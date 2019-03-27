package com.ubiquibit.buoy.jobs

import java.util.logging.Logger

import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit.{RandomElements, Spark, TopicNamer}
import com.ubiquibit.buoy._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

/**
  * Processes [[com.ubiquibit.buoy.WxStation]] station data.
  */
class WxStream(env: {
  val stationRepository: StationRepository
  val spark: Spark
}) extends Deserializer with RandomElements with TopicNamer {

  val repo: StationRepository = env.stationRepository
  val ss: SparkSession = env.spark.session
  val sc: SparkContext = env.spark.sc
  private val conf: Config = ConfigFactory.load()

  private val Log: Logger = Logger.getLogger(getClass.getName)

  def run(): Unit = {

    val stationId = StationId.makeStationId("EPTT2")
    val feedType = Text // result.get._2.get._1
    val topic: String = topicName(stationId, feedType)

    val result = for (staId <- randomElemOf(repo.readStations()))
      yield (staId.toString, staId.feeds.find(f => f._2 == DONE))

    val streamSource = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString("bootstrap.servers"))
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("spark.sql.shuffle.partitions", conf.getString("spark.partitions"))
      .load()

    import ss.implicits._
    import org.apache.spark.sql.functions._

    val enc: Encoder[TextRecord] = Encoders.product[TextRecord]

    val records: Dataset[TextRecord] = streamSource
      .map(deserialize)

    /*
      val memStore = records
        .writeStream
        .queryName(name(stationId, buoyData = feedType, None))
        .outputMode("append")
        .start()
      memStore.awaitTermination()
    */

    val timestamped = records
      .withColumn("ts_long", unix_timestamp($"eventTime"))

    /* DEBUG OUTPUT */

    val debugOut = timestamped
      .withWatermark("eventTime", "4 hours")
      .writeStream
      .format("console")
      .option("truncate", "false")
      .start

    debugOut.awaitTermination()

  }

}
