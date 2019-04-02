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
import java.util.logging.Logger

import com.ubiquibit.buoy.TextRecord
import org.apache.spark.sql.streaming.GroupState

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Arbitrary Stateful Processing for [[com.ubiquibit.buoy.WxStation]]
  *
  * @see https://databricks.com/blog/2017/10/17/arbitrary-stateful-processing-in-apache-sparks-structured-streaming.html
  */
case class StationInterrupts(var stationId: String, var lastRecord: TextRecord, var interrupts: Set[String])

object StationInterrupts {

  /**
    * Costs an iteration through the records to ensure that the caller has split the call across stationIds.
    * IF NOT, then incorrect computation will result.
    */
  private val sanityCheck = true

  val numRecords = 16

  @transient private val Log: Logger = Logger.getLogger(getClass.getName)

  def updateInterrupts(stationId: String,
                       input: Iterator[TextRecord],
                       state: GroupState[Interrupts]): Iterator[Interrupts] = {

    Log.fine(s"updateInterrupts [$stationId]")

    if (state.hasTimedOut) {
      Log.info(s"state timed out for $stationId")
      Log.fine(">>> SHORT CIRCUIT <<<")
      val existing = state.get
      state.remove()
      existing.copy(stationId = "", records = Map())
      return Iterator()
    }

    val inputValues = input.toList
    if (sanityCheck) assert(input.forall(_.stationId == stationId))
    val defaultState = Interrupts(stationId, Map())
    val newState: Interrupts = state.getOption.getOrElse(defaultState)
    Log.finer(s"newState: $newState")

    Log.finer(s">>> ${inputValues.size} input records : [$stationId] <<<")

    val processRecordsCnt = numRecords
    val retainRecordsCnt = numRecords * 2 - 1

    // need two to tango...
    if (newState.records.isEmpty && inputValues.size == 1) {
      Log.finest(">>> values size = 1 <<<")
      Log.finer(">>> SHORT CIRCUIT <<<")
      newState.stationId = stationId
      newState.records = Map(inputValues.head -> (Set[String](), Set[String]()))
      state.update(newState)
      return Iterator(newState)
    }

    Log.finer(">>> NO SHORT CIRCUIT <<<")

    val sorted = (newState.records.keys ++ inputValues).toList.sortWith(sortRecords)
    // throw away arrivals outside of our retained records window
    val retained = sorted.dropRight(sorted.size - retainRecordsCnt)
    val right = {
      if (sorted.isEmpty) List()
      else {
        val allRight = sorted.slice(processRecordsCnt - 1, sorted.size)
        allRight.dropRight(allRight.size - retainRecordsCnt)
      }
    }

    Log.finer(s"Sizes >>> sorted [${sorted.size}] ~ retained [${retained.size}] ~ right [${right.size}]")

    val procBuffer: mutable.Buffer[TextRecord] = mutable.Buffer[TextRecord]()
    procBuffer ++= retained.dropRight(retained.size - processRecordsCnt)
    Log.finer(s"Sizes >>> procBuffer [${procBuffer.size}]")

    val last = {
      if( right.nonEmpty )
        procBuffer.last -> (interrupts(right.head, procBuffer.last), onlineAgain(right.head, procBuffer.last))
      else procBuffer.last -> (Set[String](), Set[String]())
    }
    val processed = procBuffer.sliding(2)
      .map { case mutable.Buffer(a: TextRecord, b: TextRecord) => {
        a -> ( interrupts(b, a), onlineAgain(b, a))} }
      .toMap + last

    Log.finer(s"Sizes >>> processed [${processed.size}]")

    val processedRight = right.map { tr => (tr, (Set[String](), Set[String]())) }
      .toMap

    Log.finer(s"Sizes >>> processedRight [${processedRight.size}]")

    val result = Interrupts(stationId, processed ++ processedRight)
    state.update(result)
    Iterator(result)

  }

  def sortRecords(rec0: TextRecord, rec1: TextRecord): Boolean = {
    rec0.eventTime after rec1.eventTime
  }

  def updateInterruptsSimple(state: StationInterrupts, input: TextRecord): StationInterrupts = {

    // in-order - input AFTER state
    if (!input.eventTime.before(state.lastRecord.eventTime)) {

      state.interrupts = state.interrupts.diff(onlineAgain(state.lastRecord, input))
      state.interrupts = state.interrupts.union(interrupts(state.lastRecord, input))

      state.stationId = input.stationId
      state.lastRecord = input

    }

    state

  }


  private val year1969 = new Timestamp(6284160L) // 1969-12-31 19:44:44.16
  //  private val year2169 = new Timestamp(6284160000000L) // 2169-02-19 02:00:00.0
  private val defaultRecord = TextRecord(year1969, -1, "", Float.NaN, Float.NaN, Float.NaN, Float.NaN, Float.NaN, Float.NaN, Float.NaN, Float.NaN, Float.NaN, Float.NaN, Float.NaN, Float.NaN, Float.NaN, Float.NaN)

  val defaultState: StationInterrupts = StationInterrupts(
    stationId = "",
    lastRecord = defaultRecord,
    interrupts = Set[String]()
  )

  private def onlineAgain(previousRecord: TextRecord, currentRecord: TextRecord): Set[String] = {
    val result = ArrayBuffer.empty[String]

    def addOne(str: String): Unit = {
      result += str
      Log.fine(s"$str : online again - [${currentRecord.stationId}] @ ${currentRecord.eventTime}")
    }

    // TODO use Shapeless instead
    if (previousRecord.windDirection.isNaN && !currentRecord.windDirection.isNaN) addOne("windDirection")
    if (previousRecord.airTemp.isNaN && !currentRecord.airTemp.isNaN) addOne("airTemp")
    if (previousRecord.averageWavePeriod.isNaN && !currentRecord.averageWavePeriod.isNaN) addOne("averageWavePeriod")
    if (previousRecord.dewPointTemp.isNaN && !currentRecord.dewPointTemp.isNaN) addOne("dewPointTemp")
    if (previousRecord.dominantWavePeriod.isNaN && !currentRecord.dominantWavePeriod.isNaN) addOne("dominantWavePeriod")
    if (previousRecord.gustSpeed.isNaN && !currentRecord.gustSpeed.isNaN) addOne("gustSpeed")
    if (previousRecord.mWaveDirection.isNaN && !currentRecord.mWaveDirection.isNaN) addOne("mWaveDirection")
    if (previousRecord.pressureTendency.isNaN && !currentRecord.pressureTendency.isNaN) addOne("pressureTendency")
    if (previousRecord.seaLevelPressure.isNaN && !currentRecord.seaLevelPressure.isNaN) addOne("seaLevelPressure")
    if (previousRecord.waveHeight.isNaN && !currentRecord.waveHeight.isNaN) addOne("waveHeight")
    if (previousRecord.visibility.isNaN && !currentRecord.visibility.isNaN) addOne("visibility")
    if (previousRecord.tide.isNaN && !currentRecord.tide.isNaN) addOne("tide")
    if (previousRecord.windSpeed.isNaN && !currentRecord.windSpeed.isNaN) addOne("windSpeed")
    if (previousRecord.waterSurfaceTemp.isNaN && !currentRecord.waterSurfaceTemp.isNaN) addOne("waterSurfaceTemp")

    val sz = result.size
    if (sz > 0) Log.fine(s"$sz channel(s) online again : [${currentRecord.stationId}] @ ${currentRecord.eventTime}")

    result.toSet

  }

  private def interrupts(previousRecord: TextRecord, currentRecord: TextRecord): Set[String] = {

    val result = ArrayBuffer.empty[String]

    def addOne(str: String): Unit = {
      result += str
      Log.finer(s"$str : interrupted - [${currentRecord.stationId}] @ ${currentRecord.eventTime}")
    }

    Log.finest(s"comparing $previousRecord to $currentRecord")

    // TODO use Shapeless instead
    if (currentRecord.windDirection.isNaN && !previousRecord.windDirection.isNaN) addOne("windDirection")
    if (currentRecord.airTemp.isNaN && !previousRecord.airTemp.isNaN) addOne("airTemp")
    if (currentRecord.averageWavePeriod.isNaN && !previousRecord.averageWavePeriod.isNaN) addOne("averageWavePeriod")
    if (currentRecord.dewPointTemp.isNaN && !previousRecord.dewPointTemp.isNaN) addOne("dewPointTemp")
    if (currentRecord.dominantWavePeriod.isNaN && !previousRecord.dominantWavePeriod.isNaN) addOne("dominantWavePeriod")
    if (currentRecord.gustSpeed.isNaN && !previousRecord.gustSpeed.isNaN) addOne("gustSpeed")
    if (currentRecord.mWaveDirection.isNaN && !previousRecord.mWaveDirection.isNaN) addOne("mWaveDirection")
    if (currentRecord.pressureTendency.isNaN && !previousRecord.pressureTendency.isNaN) addOne("pressureTendency")
    if (currentRecord.seaLevelPressure.isNaN && !previousRecord.seaLevelPressure.isNaN) addOne("seaLevelPressure")
    if (currentRecord.waveHeight.isNaN && !previousRecord.waveHeight.isNaN) addOne("waveHeight")
    if (currentRecord.visibility.isNaN && !previousRecord.visibility.isNaN) addOne("visibility")
    if (currentRecord.tide.isNaN && !previousRecord.tide.isNaN) addOne("tide")
    if (currentRecord.windSpeed.isNaN && !previousRecord.windSpeed.isNaN) addOne("windSpeed")
    if (currentRecord.waterSurfaceTemp.isNaN && !previousRecord.waterSurfaceTemp.isNaN) addOne("waterSurfaceTemp")

    val sz = result.size
    if (sz > 0) Log.fine(s"$sz channel(s) interrupted : [${currentRecord.stationId}] @ ${currentRecord.eventTime}")

    result.toSet
  }

}
