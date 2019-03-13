package com.ubiquibit.buoy.jobs

import java.sql.Timestamp

import com.ubiquibit.buoy._
import com.ubiquibit.Spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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

    def replaceMM(str: String): String = {
      //    str.replaceAll("MM", "0.0")
      str.replaceAll("MM", "NaN")
    }

    def singleSpace(str: String): String = {
      str.replaceAll("  ", " ")
    }

    val lines = sc.textFile(fqFilename)

    val rows =
      lines
        .filter(!_.startsWith("#"))
        .map(replaceMM)
        .map(singleSpace)
        .map(singleSpace)
        .map(_.split(" "))
        .map(l => {
          val date = new Timestamp(l(0).toInt - 1900, l(1).toInt - 1, l(2).toInt, l(3).toInt, l(4).toInt, 0, 0)
          Row.fromSeq(Seq(date,
            l(5).toFloat, l(6).toFloat, l(7).toFloat, l(8).toFloat, l(9).toFloat, l(10).toFloat,
            l(11).toFloat, l(12).toFloat, l(13).toFloat, l(14).toFloat, l(15).toFloat, l(16).toFloat,
            l(17).toFloat, l(18).toFloat))
        })

    import org.apache.spark.sql.catalyst.ScalaReflection
    import org.apache.spark.sql.types._

    val schema = ScalaReflection
      .schemaFor[TextRecord]
      .dataType.asInstanceOf[StructType]

    spark.sqlContext.createDataFrame(rows, schema)
  }
}