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

import java.sql.Timestamp

import com.ubiquibit.RandomData
import com.ubiquibit.buoy.TextRecord
import org.scalatest.FunSpec

import scala.collection.mutable
import scala.util.Random

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

  describe("Interrupts case class") {

    it("has only 16 records in the window") {

      val m: mutable.Map[TextRecord, (Set[String], Set[String])] = mutable.Map()
      (0 until 30).foreach { _ => m += rec -> (Set(s), Set(s)) }

      val instance = Interrupts(stationId, records = m.toMap)

      assert(instance.inWindow().records.size == 16)
    }

    it("returns the most recent 16 records inWindow") {
      val m: mutable.Map[TextRecord, (Set[String], Set[String])] = mutable.Map()
      (0 until 30).foreach { _ => m += rec -> (Set(s), Set(s)) }

      val instance = Interrupts(stationId, records = m.toMap)

      val sortedEvents = m.keys.toList.sortWith(sortRecords)

      // sorted latest to earliest
      assert(sortedEvents.head.eventTime after sortedEvents.last.eventTime)

      val keysOutTheWindow = instance.records.keys.filter( k=> !sortedEvents.contains(k))

      keysOutTheWindow.foreach(k => assert(k.eventTime before sortedEvents.tail.head.eventTime))

    }
  }
}