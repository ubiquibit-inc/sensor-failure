package com.ubiquibit.buoy.jobs

import java.sql.{Date, Timestamp}

import com.ubiquibit.buoy.TextRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.GroupState
import org.scalatest.FunSpec

class StationInterruptsSpec extends FunSpec {

  describe("StationInterrupts") {

    import StationInterrupts._

    val stationId = "myStationInLife"

    val rec0: TextRecord = TextRecord(new Timestamp(System.currentTimeMillis()), 0, stationId, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F)
    Thread.sleep(1000)
    val rec1: TextRecord = TextRecord(new Timestamp(System.currentTimeMillis()), 0, stationId, Float.NaN, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F)
    Thread.sleep(1000)
    val rec2: TextRecord = TextRecord(new Timestamp(System.currentTimeMillis()), 0, stationId, 1F, Float.NaN, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F)

    val defaultState = StationInterrupts.defaultState
    //    val defaultRec = defaultState.lastRecord

    it("updateInterrupts should:") {

      val result0 = updateInterrupts(defaultState, rec0)
      assert(result0.interrupts.isEmpty, "have an empty default state")

      val result1 = updateInterrupts(result0, rec1)
      assert(result0.interrupts.contains("windDirection"), "detect interrupt when a channel goes from a value to NaN")

      val result2 = updateInterrupts(result1, rec2)
      assert(!result2.interrupts.contains("windDirection"), "release the interrupt when a channel goes from NaN to a real value")
      assert(result2.interrupts.contains("windSpeed"), "detect interrupt when a channel goes from value to NaN, even when another channel goes in the other direction")

    }

    /*
    it("updateInterruptsForFlatMap should:") {


      val recs = new Records(3)
      var testInterrupts: StationInterruptsForFlatMap = defaultInterruptsForFlatMap(stationId, recs)
      val testState: FakeGroupState = new FakeGroupState(testInterrupts)
      intercept[AssertionError] {
        updateInterruptsForFlatMap("bad stationId", Iterator(rec0), testState)
      }
      assert(recs.size == 0)

      val result0 = updateInterruptsForFlatMap(stationId, Iterator(rec0), testState).toList.head.records.all()
      assert(result0.contains(rec0))
      assert(result0.size === 1)

      val result1 = updateInterruptsForFlatMap(stationId, Iterator(rec1), testState).toList.head.records.all()
      assert(result1.contains(rec0))
      assert(result1.contains(rec1))
      assert(result1.size === 2)

      val result2 = updateInterruptsForFlatMap(stationId, Iterator(rec2), testState).toList.head.records.all()
      assert(result2.contains(rec0))
      assert(result2.contains(rec1))
      assert(result2.contains(rec2))
      assert(result2.size === 3)

      Thread.sleep(1000)
      val rec3: TextRecord = TextRecord(new Timestamp(System.currentTimeMillis()), 0, stationId, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F)
      testInterrupts = StationInterruptsForFlatMap(stationId, recs.push(rec3), Set("windSpeed"))
      val fakeState = new FakeGroupState(testInterrupts)
      val result3 = updateInterruptsForFlatMap(stationId, recs.all().iterator, fakeState).toList.head.records.all()
      for (tr <- recs.all()) assert(result3.contains(tr))
      assert(result3.size === 3)
      assert(!result3.contains(rec0))

    } */

  }

}

class FakeGroupState(stationInterruptsForFlatMap: StationInterruptsForFlatMap, testTimedOut: Boolean = false) extends GroupState[StationInterruptsForFlatMap] {

  override def exists: Boolean = true

  override def get: StationInterruptsForFlatMap = stationInterruptsForFlatMap

  override def getOption: Option[StationInterruptsForFlatMap] = Some(stationInterruptsForFlatMap)

  override def update(newState: StationInterruptsForFlatMap): Unit = {
//    for (tr <- newState.records.iterator()) stationInterruptsForFlatMap.records.push(tr)
  }

  override def remove(): Unit = {}

  override def hasTimedOut: Boolean = testTimedOut

  override def setTimeoutDuration(durationMs: Long): Unit = {}

  override def setTimeoutDuration(duration: String): Unit = {}

  override def setTimeoutTimestamp(timestampMs: Long): Unit = {}

  override def setTimeoutTimestamp(timestampMs: Long, additionalDuration: String): Unit = {}

  override def setTimeoutTimestamp(timestamp: Date): Unit = {}

  override def setTimeoutTimestamp(timestamp: Date, additionalDuration: String): Unit = {}

  override def getCurrentWatermarkMs(): Long = 0

  override def getCurrentProcessingTimeMs(): Long = 0

}

