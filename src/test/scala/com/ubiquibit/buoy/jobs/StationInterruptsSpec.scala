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

      val result0 = updateInterruptsSimple(defaultState, rec0)
      assert(result0.interrupts.isEmpty, "have an empty default state")

      val result1 = updateInterruptsSimple(result0, rec1)
      assert(result0.interrupts.contains("windDirection"), "detect interrupt when a channel goes from a value to NaN")

      val result2 = updateInterruptsSimple(result1, rec2)
      assert(!result2.interrupts.contains("windDirection"), "release the interrupt when a channel goes from NaN to a real value")
      assert(result2.interrupts.contains("windSpeed"), "detect interrupt when a channel goes from value to NaN, even when another channel goes in the other direction")

    }

  }

}
