/*
 * Copyright (c) 2019.
 *
 * This work, (c) by jason@ubiquibit.com
 *
 * This work is licensed under a
 * Creative Commons Attribution-ShareAlike 4.0 International License.
 *
 * You should have received a copy of the license along with this
 * work.  If not, see <http://creativecommons.org/licenses/by-sa/4.0/>.
 *
 */

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

    val stationId: StationId = StationId.makeStationId(station.getOrElse("NPXN6"))

    def notInterruptionCow(someValue: Option[Float]): TextRecord = {
      val ptdy: Float = someValue.getOrElse(Float.NaN)
      TextRecord(eventTime = new Timestamp(System.currentTimeMillis()),
        lineLength = 80,
        stationId = stationId.toString,
        windDirection = 310F,
        windSpeed = 6.2F,
        gustSpeed = Float.NaN,
        waveHeight = Float.NaN,
        dominantWavePeriod = Float.NaN,
        averageWavePeriod = Float.NaN,
        mWaveDirection = Float.NaN,
        seaLevelPressure = 1019.9F,
        airTemp = -5F,
        waterSurfaceTemp = Float.NaN,
        dewPointTemp = -17F,
        visibility = Float.NaN,
        pressureTendency = ptdy,
        tide = Float.NaN
      )
    }
    val interruptionCow = notInterruptionCow(None)

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
