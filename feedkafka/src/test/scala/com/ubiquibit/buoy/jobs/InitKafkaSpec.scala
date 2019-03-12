package com.ubiquibit.buoy.jobs

import java.sql.Timestamp


import com.ubiquibit.buoy.TextRecord

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

object InitKafkaSpec /* extends InitKafka */{

  def main(args: Array[String]): Unit = {
/*
    val hpath = supportedFiles.tail.head.getAbsolutePath

    val df = makeTextDF(hpath)

    import spark.sqlContext.implicits._
    df.orderBy($"eventTime".desc)
    df.show()

    df.printSchema()
*/
  }

/*  override def makeTextDF(fqFilename: String): DataFrame = {

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

    import org.apache.spark.sql.types._
    import org.apache.spark.sql.catalyst.ScalaReflection

    val schema = ScalaReflection
      .schemaFor[TextRow]
      .dataType.asInstanceOf[StructType]

    spark.sqlContext.createDataFrame(rows, schema)

  }

  override def replaceMM(str: String): String = {
    //    str.replaceAll("MM", "0.0")
    str.replaceAll("MM", "NaN")
  }

  override def singleSpace(str: String): String = {
    str.replaceAll("  ", " ")
  }
*/
}