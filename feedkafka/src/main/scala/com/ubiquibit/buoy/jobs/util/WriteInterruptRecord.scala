package com.ubiquibit.buoy.jobs.util

import java.sql.Timestamp
import java.util.logging.Logger

import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit.buoy.{StationId, Text, TextRecord}
import com.ubiquibit.{Spark, TopicNamer, Wiring}
import org.apache.spark.sql.SparkSession

/**
  * A little utility to write a few test records to Kafka...
  */
class WriteInterruptRecord(env: {
  val spark: Spark
}) extends TopicNamer {

  val ss: SparkSession = env.spark.session
  @transient private val Log: Logger = Logger.getLogger(getClass.getName)
  private val conf: Config = ConfigFactory.load()

  def run(station: Option[String]): Unit = {

    val stationId: StationId = StationId.makeStationId(station.getOrElse("46082"))
    val interruptionCow: TextRecord = TextRecord(new Timestamp(System.currentTimeMillis()), 80, stationId.toString,
      0F, 0F, 0F, 0F, 0F, 0F, 0F, seaLevelPressure = Float.NaN, 0F, 0F, 0F, 0F, 0F, 0F
    )

    import ss.implicits._

    val ds = ss
      .createDataFrame(Seq(interruptionCow))
      .as[TextRecord]

    ds.printSchema()

    val topic: String = topicName(stationId, Text)
    Log.info(s"Writing an interrupt record to Kafka topic = $topic.")

    ds.map(_.valueOf())
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString("bootstrap.servers"))
      .option("topic", topic)
      .save()

    val cnt = ds.count()

    Log.info(s"Wrote $cnt records to Kafka topic = [$topic].")
    Log.info("Goodbye, cruel world...")

  }

}

object WriteInterruptRecord {
  def main(args: Array[String]): Unit = {
    var stationId: Option[String] = None
    if (args.length > 0) stationId = Some(args(0))

    Wiring.writeInterruptRecord.run(stationId)
  }
}
