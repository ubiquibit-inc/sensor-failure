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

package com.ubiquibit.buoy.parse

import java.sql.Timestamp

import com.ubiquibit.buoy.{StationId, TextRecord}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.annotation.tailrec


/**
  * Parses files into DataFrames
  */
sealed abstract class BuoyDataParser {

  /**
    * @param fqFileName a pointer to a
    * @param spark      a spark session container
    * @return a fully-processed DataFrame, in ascending eventTime order (earliest to latest)
    */
  def parseFile(fqFileName: String)(implicit spark: SparkSession): DataFrame

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

class TextParser(stationId: String) extends BuoyDataParser with Serializable {

  override def parseFile(fqFileName: String)(implicit spark: SparkSession): DataFrame = {

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

    import org.apache.spark.sql.functions._

    spark.sqlContext.createDataFrame(rows, schema)
      .orderBy(asc("eventTime"))

  }

  override def processLine(line: String): Row = {
    val len = line.length
    val l = line.split(" ")
    val date = new Timestamp(l(0).toInt - 1900, l(1).toInt - 1, l(2).toInt, l(3).toInt, l(4).toInt, 0, 0)
    Row.fromSeq(Seq(date,
      len,
      stationId,
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