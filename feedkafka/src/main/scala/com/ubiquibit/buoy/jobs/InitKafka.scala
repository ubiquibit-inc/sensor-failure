package com.ubiquibit.buoy.jobs

import java.sql.Timestamp

import com.ubiquibit.buoy._
import com.ubiquibit.Spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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
}) extends InitKafka{

  private val repo: StationRepository = env.stationRepository
  private val spark: SparkSession = env.spark.spark
  private val sc: SparkContext = env.spark.sc
  private val filez: FileReckoning = env.fileReckoning

  def run(): Unit ={
    for ((sta, ft) <- filez.supportByStation) {
      ft.foreach{ t =>
        val f = filez.getFile(sta, t).get
        val path = f.getAbsolutePath
        val df = textToDF(path)
        println(s"Created dataframe for $path")
        df.show(30, truncate = false)
      }
    }
  }

  def textToDF(fqFilename: String): DataFrame = {
    ???
  }
}