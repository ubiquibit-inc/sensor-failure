package com.ubiquibit.buoy.parse

import java.sql.Timestamp

import com.ubiquibit.buoy.TextRecord
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.annotation.tailrec


/**
  * Parses files into DataFrames
  */
sealed abstract class BuoyDataParser {

  /**
    * @param fqFileName a pointer to a
    * @param spark      a spark session container
    * @return a fully-processed DataFrame
    */
  def parse(fqFileName: String)(implicit spark: SparkSession): DataFrame

  /**
    * Processes a single line from input
    *
    * @param line a line
    * @return a Row
    */
  def processLine(line: String): Row

}

object Parsers {

  @tailrec
  def singleSpace(str: String): String = {

    val single = " "
    val double = s"$single$single"

    def hasDouble(s: String): Boolean = s.indexOf(double) != -1

    def replaceDouble(s: String): String = str.replaceAll(double, single)

    if (!hasDouble(str)) str
    else singleSpace(replaceDouble(str))

  }

  def replaceMM(str: String): String = {
    //    str.replaceAll("MM", "0.0")
    str.replaceAll("MM", "NaN")
  }


}

class TextParser extends BuoyDataParser with java.io.Serializable {

  override def parse(fqFileName: String)(implicit spark: SparkSession): DataFrame = {

    val lines = spark.sparkContext.textFile(fqFileName)

    val rows =
      lines
        .filter(!_.startsWith("#"))
        .map(Parsers.replaceMM)
        .map(Parsers.singleSpace)
        .map(processLine)

    import org.apache.spark.sql.catalyst.ScalaReflection
    import org.apache.spark.sql.types._

    val schema = ScalaReflection
      .schemaFor[TextRecord]
      .dataType.asInstanceOf[StructType]

    spark.sqlContext.createDataFrame(rows, schema)

  }

  override def processLine(line: String): Row = {
    val len = line.length
    val l = line.split(" ")
    val date = new Timestamp(l(0).toInt - 1900, l(1).toInt - 1, l(2).toInt, l(3).toInt, l(4).toInt, 0, 0)
    Row.fromSeq(Seq(date,
      len,
      l(5).toFloat,
      l(6).toFloat,
      l(7).toFloat,
      l(8).toFloat,
      l(9).toFloat,
      l(10).toFloat,
      l(11).toFloat,
      l(12).toFloat,
      l(13).toFloat,
      l(14).toFloat,
      l(15).toFloat,
      l(16).toFloat,
      l(17).toFloat,
      l(18).toFloat
    ))
  }

}