package com.ubiquibit.buoy.jobs

import java.util.logging.Logger

import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit.{RandomElements, Spark, TopicNamer, Wiring}
import com.ubiquibit.buoy._
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class StationFeed(stationId: String, feedType: String)

/**
  * Processes [[com.ubiquibit.buoy.WxStation]] station data.
  */
class WxStream(env: {
  val spark: Spark
}) extends Deserializer with RandomElements with TopicNamer {

  val ss: SparkSession = env.spark.session
  val sc: SparkContext = env.spark.sc
  private val conf: Config = ConfigFactory.load()

  private val Log: Logger = Logger.getLogger(getClass.getName)

  def run(): Unit = {

    import ss.implicits._
    import org.apache.spark.sql.Encoders._
    import org.apache.spark.sql.functions._

    val schema = StructType(
      StructField("stationId", StringType, false) ::
        StructField("feedType", StringType, false) :: Nil
    )
    val enc: Encoder[StationFeed] = Encoders.product[StationFeed]

    val sf1 = ss.readStream
      .option("header", false)
      .schema(schema = schema)
      .csv(path = conf.getString("stage.dir"))
      .as(enc)

    val debugOut = sf1
      .writeStream
      .format("console")
      .option("truncate", "false")
      .start

    debugOut.awaitTermination()

  }

}

object WxStream {
  def main(args: Array[String]): Unit = {
    Wiring.wxStream.run()
  }
}

//    stationFeeds.foreach(println(_))

/*
x.map(info => (StationId.makeStationId(info.stationId), BuoyFeed.valueOf(info.feedType))


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
  */

/*
  val memStore = records
    .writeStream
    .queryName(name(stationId, buoyData = feedType, None))
    .outputMode("append")
    .start()
  memStore.awaitTermination()
*/

/*
val timestamped = records
  .withColumn("ts_long", unix_timestamp($"eventTime"))
  */
