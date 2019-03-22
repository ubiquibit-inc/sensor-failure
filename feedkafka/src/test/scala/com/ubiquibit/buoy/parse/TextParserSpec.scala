package com.ubiquibit.buoy.parse

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Path}

import com.ubiquibit._
import com.ubiquibit.buoy.TextRecord
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfter

class TextParserSpec extends SparkSpec with BeforeAndAfter {

  val instance = new TextParser

  val exampleTextData: String =
    """|#YY  MM DD hh mm WDIR WSPD GST  WVHT   DPD   APD MWD   PRES  ATMP  WTMP  DEWP  VIS PTDY  TIDE
       |#yr  mo dy hr mn degT m/s  m/s     m   sec   sec degT   hPa  degC  degC  degC  nmi  hPa    ft
       |2019 03 09 19 00  MM   MM   MM    MM    MM    MM  MM 1021.2  15.5  11.7    MM   MM -1.3    MM""".stripMargin

  // Note: Some of this wrangling is necessary to simulate...
  val Array(header0, header1, payload) = exampleTextData
    .split("\\r?\\n")
    .map(_.replaceAll("  ", " "))
    .map(_.replaceAll("  ", " "))
    .map(_.replaceAll("MM", "NaN"))

  val epsilon = 1e-4f

  var file: File = new File("")

  before {
    val path: Path = Files.createTempFile("textparserspec", ".txt")
    file = new File(path.toString)
    val pw = new PrintWriter(file)
    pw.write(exampleTextData)
    pw.close()
  }

  describe("TextParser should") {

    it("parse a single record") {

      val row: Row = instance.processLine(payload)

      val t = row.getTimestamp(0)
      assert(TimeHelper.getYear(t) === 2019)
      assert(TimeHelper.getMonth(t) === 2)
      assert(TimeHelper.getDayOfMonth(t) === 9)

      val len = row.getInt(1)
      assert(len == 78)

      def f(idx: Int): Float = {
        val n = row.getFloat(idx)
        if (n.equals(Float.NaN)) n
        else BigDecimal(n).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat
      }


      assert(f(2).isNaN)
      assert(f(3).isNaN)
      assert(f(3).isNaN)
      assert(f(4).isNaN)
      assert(f(5).isNaN)
      assert(f(6).isNaN)
      assert(f(7).isNaN)
      assert(f(8).isNaN)

      assert(Math.abs(f(9) - 1021.2) < epsilon)
      assert(Math.abs(f(10) - 15.5) < epsilon)
      assert(Math.abs(f(11) - 11.7) < epsilon)

      assert(f(12).isNaN)
      assert(f(13).isNaN)

      assert(Math.abs(f(14) + 1.3) < epsilon)
      assert(f(15).isNaN)
    }

    def problemFile = {
      val filename = "BDRN4.txt"
      val ddir = "/Users/jason/scratch/sensor-failure/data/www.ndbc.noaa.gov/data/realtime2"
      val fqFn = s"$ddir/$filename"

      println("FN >>> " + fqFn)
      fqFn
    }

    it("parses a problem file...", SparkTest) { // turned off for speediness


      val pFile: String = problemFile

      implicit val spark: SparkSession = ss

      import spark.implicits._

      val df = instance.parse(problemFile)

      println("" + df.count() + " of " + pFile + " processed.")

    }

    it("records line width", SparkTest) {

      val file: String = problemFile

      implicit val spark: SparkSession = ss

      import spark.implicits._

      val ds = instance
        .parse(file)
        .as[TextRecord]

      val firstThree = ds.take(3)
      assert(firstThree(0).lineLength === 74)
      assert(firstThree(1).lineLength === 75)
      assert(firstThree(2).lineLength === 75)

    }

  }

}
