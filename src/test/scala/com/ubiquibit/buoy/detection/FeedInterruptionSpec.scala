package com.ubiquibit.buoy.detection

import java.sql.Timestamp

import com.ubiquibit.TimeHelper
import com.ubiquibit.buoy.TextRecord
import org.scalatest.FunSpec

import scala.util.Random

class FeedInterruptionSpec extends FunSpec {

  /*
  val instance: FeedInterruption = new Object with FeedInterruption

  describe("FeedInterruption") {

    def makeRecord(eventTime: Timestamp, windDirection: Float, windSpeed: Float): TextRecord = {
      def f = math.abs(Random.nextFloat())

      TextRecord(eventTime, 20, windDirection, windSpeed, f, f, f, f, f, f, f, f, f, f, f, f)
    }

    def nextTime(someTime: Timestamp): Timestamp = {
      val offset = 10 * 1000000 * 60 // 10 mins
      new Timestamp(someTime.getTime + offset)
    }


    def testRecords(windDirection0: Float = 123F, windDirection1: Float = 234F, windSpeed0: Float = 345F, windSpeed1: Float = 456F): (TextRecord, TextRecord) = {
      val t0 = TimeHelper.epochTimeZeroTimestamp()

      val rec0: TextRecord = makeRecord(t0, windDirection0, windSpeed0)
      val rec1: TextRecord = makeRecord(nextTime(t0), windDirection1, windSpeed1)

      (rec0, rec1)
    }


    it("returns empty when a channel has data on successive records") {

      val (rec0, rec1) = testRecords()

      assert(instance.detect(rec0, rec1).isEmpty)

    }

    it("returns empty when a channel is NaN on successive records") {
      val (rec0, rec1) = testRecords(windDirection0 = Float.NaN, windDirection1 = Float.NaN)

      assert(instance.detect(rec0, rec1).isEmpty)
    }

    it("returns Some Interruption on the correct channel when there is one") {

      val (rec0, rec1) = testRecords(windDirection1 = Float.NaN)

      val result = instance.detect(rec0, rec1)

      assert(result.exists(irpt => irpt.channel == "windDirection"))

    }

    it("returns Interruptions on multiple channels") {

      val (rec0, rec1) = testRecords(windDirection1 = Float.NaN, windSpeed1 = Float.NaN)

      val result = instance.detect(rec0, rec1)

      assert(result.exists(irpt => irpt.channel == "windDirection"))
      assert(result.exists(irpt => irpt.channel == "windSpeed"))

    }


  }
  */
}