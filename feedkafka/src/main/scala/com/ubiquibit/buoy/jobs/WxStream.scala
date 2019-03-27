package com.ubiquibit.buoy.jobs

import java.util.logging.Logger

import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit.{RandomElements, Spark, TopicNamer, Wiring}
import com.ubiquibit.buoy._
import com.ubiquibit.buoy.serialize.DefSer
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

  @transient val ss: SparkSession = env.spark.session
  @transient val sc: SparkContext = env.spark.sc
  private val conf: Config = ConfigFactory.load()

  @transient private val Log: Logger = Logger.getLogger(getClass.getName)

  def run(): Unit = {

    import ss.implicits._
    import org.apache.spark.sql.Encoders._
    import org.apache.spark.sql.functions._

    val schema = StructType(
      StructField("stationId", StringType, false) ::
        StructField("feedType", StringType, false) :: Nil
    )
    val enc: Encoder[StationFeed] = Encoders.product[StationFeed]

    val topics = ss.read
      .option("header", false)
      .schema(schema = schema)
      .csv(path = conf.getString("stage.dir"))
      .as(enc)
      .map(sf => topicName(StationId.makeStationId(sf.stationId), WxFeed.valueOf(sf.feedType).get))
      .selectExpr("value AS topic")
      .select('topic)
      .map(t => t.getString(0)).collect().mkString(",")

    Log.info(s"Reading topics: $topics")

    val kafkaFeed = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString("bootstrap.servers"))
      .option("subscribe", topics)
      .option("startingOffsets", "earliest")
      .option("spark.sql.shuffle.partitions", conf.getString("spark.partitions"))
      .load()

    val recordStream = kafkaFeed.map(row =>
      DefSer.deserialize(row.getAs[Array[Byte]]("value")).asInstanceOf[TextRecord]
    )

//    recordStream.printSchema()

    val debugOut = recordStream
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


/*
import ss.implicits._
import org.apache.spark.sql.functions._

val enc: Encoder[TextRecord] = Encoders.product[TextRecord]

  val memStore = records
    .writeStream
    .queryName(name(stationId, buoyData = feedType, None))
    .outputMode("append")
    .start()
  memStore.awaitTermination()
val timestamped = records
  .withColumn("ts_long", unix_timestamp($"eventTime"))
  */
