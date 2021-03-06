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

package com.ubiquibit.buoy.jobs

import java.util.logging.Logger

import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit._
import com.ubiquibit.buoy._
import com.ubiquibit.buoy.serialize.DefSer
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.streaming._
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

  import WxStream._

  def run(): Unit = {

    import ss.implicits._
    import org.apache.spark.sql.Encoders._
    import org.apache.spark.sql.functions._

    val enc: Encoder[StationFeed] = Encoders.product[StationFeed]

    SparkSession.setActiveSession(ss)

    val topics = ss.read
      .option("header", value = false)
      .schema(schema = stationFeedSchema)
      .csv(path = conf.getString("stage.dir"))
      .as(enc)
      .map(sf => topicName(StationId.makeStationId(sf.stationId), WxFeed.valueOf(sf.feedType).get))
      .selectExpr("value AS topic")
      .select('topic)

    val topicString = topics.map(t => t.getString(0))
      .collect()
      .mkString(",")
    Log.info(s"Reading topics: $topicString")

    val kafkaFeed = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString("bootstrap.servers"))
      .option("subscribe", topicString)
      .option("startingOffsets", "earliest")
      .option("spark.sql.shuffle.partitions", conf.getString("spark.partitions"))
      .load()

    import StationInterrupts._
    import scala.concurrent.duration._

    val recordsByStationId = kafkaFeed
      .map(deserialize)
      .groupByKey(_.stationId)

    val interruptScanner: Dataset[Interrupts] = recordsByStationId.flatMapGroupsWithState(
      outputMode = OutputMode.Append,
      timeoutConf = GroupStateTimeout.NoTimeout)(func = updateInterrupts)

    val orcOut = interruptScanner
      .filter(_.isInterrupted)
      .flatMap(i => {
        val allRecords = i.records.keys.toList.sortWith(sortRecords)
        val interrupted = i.records.filter(_._2._1.nonEmpty)
        for (irp <- interrupted) yield {
          val rec = irp._1
          val pos = allRecords.indexOf(rec)
          allRecords.slice(pos, pos + StationInterrupts.numRecords)
        }
      })
      .writeStream
      .format("orc")
      .option("path", conf.getString("orc.output.path"))
      .foreachBatch { (batchDs: Dataset[List[TextRecord]], batchId: Long) =>
        val stationId = batchDs.first.head.stationId
        batchDs.persist()
        batchDs.write
          .format("orc")
          .option("path", s"${conf.getString("orc.output.path")}/$stationId}/")
          .save()
      }
      .start()

    orcOut.awaitTermination()

    // Interrupts(var stationId: String, var records: Map[TextRecord, (Set[String], Set[String])])
    val interruptedOutput = interruptScanner
      .filter(_.isInterrupted)
      .map(_.inWindow())
      .withColumn("interrupted", lit("INTERRUPTED"))
      .writeStream
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime(8.second))
      .outputMode(OutputMode.Append)
      .start

    val onlineAgainOut = interruptScanner
      .filter(_.isOnlineAgain)
      .map(_.inWindow())
      .withColumn("online", lit("ONLINE-AGAIN"))
      .writeStream
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime(8.second))
      .outputMode(OutputMode.Append)
      .start

    interruptedOutput.awaitTermination()
    onlineAgainOut.awaitTermination()

  }

}

object WxStream extends TopicNamer {

  def nameForFeed(stationFeed: StationFeed): Option[(String, WxFeed)] = {
    val feed: Option[WxFeed] = WxFeed.valueOf(stationFeed.feedType)
    if (feed.isDefined)
      Some((stationFeed.stationId, feed.get))
    else None
  }

  val stationFeedSchema: StructType = StructType(
    StructField("stationId", StringType, nullable = false) ::
      StructField("feedType", StringType, nullable = false) :: Nil
  )

  def deserialize(row: Row): TextRecord = {
    DefSer.deserialize(row.getAs[Array[Byte]]("value")).asInstanceOf[TextRecord]
  }

  def main(args: Array[String]): Unit = {
    Wiring.wxStream.run()
  }

}