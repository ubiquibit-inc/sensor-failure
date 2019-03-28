package com.ubiquibit.buoy.jobs

import java.sql.Timestamp

import com.ubiquibit.buoy.TextRecord
import org.scalatest.{FunSpec, FunSuite}

class StationInterruptsSpec extends FunSpec {

  describe("StationInterrupts") {

    it("should initialize") {

      val defaultState = StationInterrupts.defaultState
      val defaultRec = defaultState.lastRecord

      val rec0: TextRecord = TextRecord(new Timestamp(System.currentTimeMillis()), 0, "myStationInLife", 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F)
      Thread.sleep(1000)
      val rec1: TextRecord = TextRecord(new Timestamp(System.currentTimeMillis()), 0, "myStationInLife", Float.NaN, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F)
      Thread.sleep(1000)
      val rec2: TextRecord = TextRecord(new Timestamp(System.currentTimeMillis()), 0, "myStationInLife", 1F, Float.NaN, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F)

      val result0 = StationInterrupts.updateInterruptsWithEvent(defaultState, rec0)
      assert(result0.interrupts.isEmpty)
      val result1 = StationInterrupts.updateInterruptsWithEvent(result0, rec1)
      assert(result0.interrupts.contains("windDirection"))
      val result2 = StationInterrupts.updateInterruptsWithEvent(result1, rec2)
      assert(!result2.interrupts.contains("windDirection"))
      assert(result2.interrupts.contains("windSpeed"))

    }
  }

}
