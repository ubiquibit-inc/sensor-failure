package com.ubiquibit.buoy.jobs

import java.sql.Timestamp

import com.ubiquibit.buoy._
import com.ubiquibit.Spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * This is intended to read up data from disk, wrangle the bejeebers out of it,
  * and present it in pristine form to the eventual consumer (on the other side of the
  * Kafka queue).
  *
  * It's a one-off operation: It just reads what's there (in the form of "supported"
  * file formats) and exits.
  *
  * Subsequent writes to Kafka will be through another mechanism.
  */
trait InitKafka {

  //   c. write data to Kafka (using canonical model)
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