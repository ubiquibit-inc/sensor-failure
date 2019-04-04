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

import java.io._
import java.nio.file.{Files, Path}

import com.ubiquibit._
import com.ubiquibit.buoy.{CManId, StationId, TextRecord}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfter

import scala.io.{BufferedSource, Source}

class TextParserSpec extends SparkSpec with BeforeAndAfter {

  val testStationId = StationId.makeStationId("xyzpdq")

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

      val instance = new TextParser(testStationId.toString)

      val row: Row = instance.processLine(payload)

      val t = row.getTimestamp(0)
      assert(TimeHelper.getYear(t) === 2019)
      assert(TimeHelper.getMonth(t) === 2)
      assert(TimeHelper.getDayOfMonth(t) === 9)

      val len = row.getInt(1)
      assert(len == 78)

      val stationId = row.getString(2)
      assert(stationId === testStationId.toString)

      def f(idx: Int): Float = {
        val n = row.getFloat(idx)
        if (n.equals(Float.NaN)) n
        else BigDecimal(n).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat
      }


      assert(f(3).isNaN)
      assert(f(3).isNaN)
      assert(f(4).isNaN)
      assert(f(5).isNaN)
      assert(f(6).isNaN)
      assert(f(7).isNaN)
      assert(f(8).isNaN)
      assert(f(9).isNaN)

      assert(Math.abs(f(10) - 1021.2) < epsilon)
      assert(Math.abs(f(11) - 15.5) < epsilon)
      assert(Math.abs(f(12) - 11.7) < epsilon)

      assert(f(13).isNaN)
      assert(f(14).isNaN)

      assert(Math.abs(f(15) + 1.3) < epsilon)
      assert(f(16).isNaN)
    }

    def problemFile = {
      val filename = "BDRN4.txt"
      val src = Source.fromURL(getClass.getResource(s"/$filename"))

      val tmpDir = java.lang.System.getProperty("java.io.tmpdir")
      val tmpFile = File.createTempFile(getClass.getName, "test", new File(tmpDir))
      println("FN >>> " + tmpFile.getAbsolutePath)

      val reader = src.bufferedReader
      val writer = new PrintWriter(tmpFile)

      def rw(): Unit = {
        val line = reader.readLine()
        if (line != null && line.nonEmpty) {
          writer.write(line)
          writer.write("\n")
          rw()
        }
      }

      rw()

      reader.close()
      writer.close()

      tmpFile.getAbsolutePath
    }

    val problemStationId: StationId = CManId("BD", "RN4")
    val problemFileInstance = new TextParser(problemStationId.toString)

    it("parses a problem file...", SparkTest) { // turned off for speediness

      val pFile = problemFile

      implicit val spark: SparkSession = ss

      import spark.implicits._

      val df = problemFileInstance.parseFile(pFile)

      val cnt = df.count()

      assert(cnt === 10959)

    }

    it("records line width", SparkTest) {

      val file: String = problemFile

      implicit val spark: SparkSession = ss

      import spark.implicits._

      val ds = problemFileInstance
        .parseFile(file)
        .as[TextRecord]

      val firstThree = ds.take(3)
      assert(firstThree(0).lineLength === 76)
      assert(firstThree(1).lineLength === 75)
      assert(firstThree(2).lineLength === 75)

    }

    it("sorts records chronologically (earliest at the top)", SparkTest) {

      val file: String = problemFile

      implicit val spark: SparkSession = ss

      import spark.implicits._

      val ds = problemFileInstance
        .parseFile(file)
        .as[TextRecord]

      val some = ds.take(16).toList

      for ((List(first, second)) <- some.sliding(2)) {
        assert(first.eventTime.before(second.eventTime))
      }

    }

  }

}
