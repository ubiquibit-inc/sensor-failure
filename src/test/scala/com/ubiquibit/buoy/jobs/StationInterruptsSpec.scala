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

import java.sql.{Date, Timestamp}

import com.ubiquibit.RandomData
import com.ubiquibit.buoy.TextRecord
import org.apache.spark.sql.streaming.GroupState
import org.scalatest.FunSpec

class StationInterruptsSpec extends FunSpec with RandomData {

  val stationId = "myStationInLife"

  import StationInterrupts._

  describe("StationInterrupts") {

    val rec0: TextRecord = TextRecord(new Timestamp(System.currentTimeMillis()), 0, stationId, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F)
    Thread.sleep(1000)
    val rec1: TextRecord = TextRecord(new Timestamp(System.currentTimeMillis()), 0, stationId, Float.NaN, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F)
    Thread.sleep(1000)
    val rec2: TextRecord = TextRecord(new Timestamp(System.currentTimeMillis()), 0, stationId, 1F, Float.NaN, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F)

    val defaultState = StationInterrupts.defaultState
    //    val defaultRec = defaultState.lastRecord

    it("updateInterruptsSimple") {

      val result0 = updateInterruptsSimple(defaultState, rec0)
      assert(result0.interrupts.isEmpty, "have an empty default state")

      val result1 = updateInterruptsSimple(result0, rec1)
      assert(result0.interrupts.contains("windDirection"), "detect interrupt when a channel goes from a value to NaN")

      val result2 = updateInterruptsSimple(result1, rec2)
      assert(!result2.interrupts.contains("windDirection"), "release the interrupt when a channel goes from NaN to a real value")
      assert(result2.interrupts.contains("windSpeed"), "detect interrupt when a channel goes from value to NaN, even when another channel goes in the other direction")

    }

  }

  describe("updateInterrupts") {

    it("returns text record input inside of the output") {

      val stationId = s

      val r0 = rec(Some(stationId))
      val r1 = rec(Some(stationId))
      val r2 = rec(Some(stationId))

      val output = updateInterrupts(s, Iterator(r0, r1, r2), new FakeGroupState()).toList

      assert(output.exists(irps => irps.records.keys.exists(_.equals(r0))))
      assert(output.exists(irps => irps.records.keys.exists(_.equals(r1))))
      assert(output.exists(irps => irps.records.keys.exists(_.equals(r2))))

    }

    it("returns an interrupt notification for interrupted records") {

      val r0 = rec(None)
      val r1 = TextRecord(ts, i, r0.stationId, windDirection = Float.NaN, f, f, f, f, f, f, f, f, f, f, f, f, f)
      val r2 = TextRecord(ts, i, r0.stationId, f, f, f, f, f, f, f, f, f, f, f, f, f, f)

      val output: Interrupts = updateInterrupts(r0.stationId, Iterator(r0, r1, r2), new FakeGroupState).toList.head

      def doAsserts(out: Interrupts) = {
        assert(out.records(r0)._1.isEmpty)
        assert(out.records(r0)._2.isEmpty)

        assert(out.records(r1)._1.size == 1)
        assert(out.records(r1)._1.contains("windDirection"))
        assert(out.records(r1)._2.isEmpty)

        assert(out.records(r2)._1.isEmpty)
        assert(out.records(r2)._2.size == 1)
        assert(out.records(r2)._2.contains("windDirection"))
      }

      doAsserts(output)

      val output2 = updateInterrupts(r0.stationId, Iterator(r2, r1, r0), new FakeGroupState).toList.head
      doAsserts(output2)
      val output3 = updateInterrupts(r0.stationId, Iterator(r1, r2, r0), new FakeGroupState).toList.head
      doAsserts(output3)
      val output4 = updateInterrupts(r0.stationId, Iterator(r2, r0, r1), new FakeGroupState).toList.head
      doAsserts(output4)

    }

    it("handles the boundary case where there are exactly 16 records"){

      val stationId = s
      val testRecs = testRecords(Some(16), Some(stationId)).sortWith(sortRecords)

      val output = updateInterrupts(s, testRecs.iterator, new FakeGroupState).toList.head

      output.records(testRecs.last)._1.isEmpty
      output.records(testRecs.last)._2.isEmpty

    }

  }

}

class FakeGroupState extends GroupState[Interrupts]() {
  override def exists: Boolean = true

  override def get: Interrupts = ???

  override def getOption: Option[Interrupts] = None

  override def update(newState: Interrupts): Unit = {}

  override def remove(): Unit = ???

  override def hasTimedOut: Boolean = false

  override def setTimeoutDuration(durationMs: Long): Unit = ???

  override def setTimeoutDuration(duration: String): Unit = ???

  override def setTimeoutTimestamp(timestampMs: Long): Unit = ???

  override def setTimeoutTimestamp(timestampMs: Long, additionalDuration: String): Unit = ???

  override def setTimeoutTimestamp(timestamp: Date): Unit = ???

  override def setTimeoutTimestamp(timestamp: Date, additionalDuration: String): Unit = ???

  override def getCurrentWatermarkMs(): Long = ???

  override def getCurrentProcessingTimeMs(): Long = ???
}