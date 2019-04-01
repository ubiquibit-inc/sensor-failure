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

case class StationInterruptsForFlatMap(var stationId: String, var records: ArrayBuffer[TextRecord], var interrupts: Set[String])

/**
  * @param stationId event source
  * @param records map from individual event to (channel interrupts, re-activation events) on that tick
  */
case class Interrupts(var stationId: String, var records: Map[TextRecord, (Set[String], Set[String])]) {

  private val processRecordsCnt = StationInterrupts.numRecords
  import StationInterrupts._

  def isInterrupted: Boolean = records.exists(_._2._1.nonEmpty)

  def isOnlineAgain: Boolean = records.exists(_._2._2.nonEmpty)

  def inWindow(): Interrupts = {
    val keepers = records.keys.toList.sortWith(sortRecords).dropRight(records.size - processRecordsCnt)
    Interrupts(stationId, records.filterKeys(k=> keepers.contains(k)))
  }

}

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

    val processed = procBuffer.sliding(2)
      .map { case mutable.Buffer(a: TextRecord, b: TextRecord) => a -> (interrupts(a, b), onlineAgain(a, b)) }
      .toMap

    Log.finer(s"Sizes >>> processed [${processed.size}]")

    val processedRight = right.map { tr => (tr, (Set[String](), Set[String]())) }
      .toMap

    Log.finer(s"Sizes >>> processedRight [${processedRight.size}]")

    val result = Interrupts(stationId, processed ++ processedRight)
    state.update(result)
    Iterator(result)

  }

  def defaultInterruptsForFlatMap(stationId: String, records: ArrayBuffer[TextRecord]): StationInterruptsForFlatMap = StationInterruptsForFlatMap(stationId, records, Set())

  def updateInterruptsForFlatMap(stationId: String,
                                 recordsPerStation: Iterator[TextRecord],
                                 state: GroupState[StationInterruptsForFlatMap]): Iterator[StationInterruptsForFlatMap] = {

    if (state.hasTimedOut) {
      val existing = state.get
      state.remove()
      existing.copy(records = new ArrayBuffer, interrupts = Set()) // release old pointers...
      return Iterator()
    }

    val values = recordsPerStation.toList
    if (sanityCheck) assert(values.forall(_.stationId == stationId))
    val initialState: StationInterruptsForFlatMap = defaultInterruptsForFlatMap(stationId, new ArrayBuffer)
    val newState: StationInterruptsForFlatMap = state.getOption.getOrElse(initialState) // records.size == 0

    Log.fine(s"${values.size} input records : [$stationId]")

    for (tr <- values) newState.records :+ tr

    Log.finer(s"First record before sort: ${newState.records.headOption}")
    newState.records = newState.records.sortWith(sortRecords)
    Log.finer(s"First record after sort: ${newState.records.headOption}")

    Log.finer(s"records size before dropRight: ${newState.records.size}")
    if (newState.records.size > numRecords)
      newState.records = newState.records.dropRight(newState.records.size - numRecords)
    Log.finer(s"records size after dropRight: ${newState.records.size}")

    Log.finer(s"Printing [$stationId] records soon...")
    newState.records.zipWithIndex.foreach { case (v, idx) => Log.finest(s"$idx: $v") }
    Log.finer(s"Printed [$stationId] records.")

    /* TODO: Think!
     *
     * The following logic is problematic in that - while it captures all in-window
     * interruptions, the way we are reporting GroupState back out through
     * flatMapGroupsWithState doesn't really make it obvious which record caused the interrupt
     * in the case of out-of-order record arrival.
     *
     * It may not be a problem to know that "last if possibly out of order record" caused an
     * interrupt, but it may introduce downstream skew into ML models since we don't "rewind"
     * the frame of reference to a set number of records per interruption.
     */
    if (newState.records.size > 1) {

      Log.finest(s"More than 1 [$stationId] newState record")

      val current = newState.interrupts

      val off = newState.records
        .sliding(2)
        .map { case ArrayBuffer(a: TextRecord, b: TextRecord) => interrupts(a, b) }
        .foldLeft(Set[String]())((r, c) => r.union(c))

      val offSz = off.size
      if (offSz > 0) Log.finer(s"""$offSz channel(s) interrupted: [$stationId] = $off""")

      val on = newState.records
        .sliding(2)
        .map { case Seq(a: TextRecord, b: TextRecord) => onlineAgain(a, b) }
        .foldLeft(Set[String]())((r, c) => r.union(c))

      val onSz = on.size
      if (onSz > 0) Log.fine(s"""$onSz channel(s) online again: [$stationId] = $on""")
      if (onSz > 0 || offSz > 0) {
        Log.finer(s"current = $current")
        Log.finer(s"off -- on = ${off -- on}")
      }

      newState.interrupts = current ++ off -- on

    }
    // records.size == 1
    else {
      Log.finest(s"Just 1 [$stationId] newState record")
      newState.interrupts = Set()
    }

    Log.finest(s"Printing [$stationId] interrupts soon...")
    newState.interrupts.zipWithIndex.foreach { case (v, idx) => Log.finer(s"$idx: $v") }
    Log.finest(s"Printed [$stationId] interrupts.")

    state.update(newState)
    Iterator(newState)

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

  def updateAcrossEvents(stationId: String,
                         inputs: Iterator[TextRecord],
                         oldState: GroupState[StationInterrupts]): StationInterrupts = {

    var state: StationInterrupts = if (oldState.exists) oldState.get else defaultState

    for (input <- inputs) {
      state = updateInterruptsSimple(state, input)
      oldState.update(state)
    }

    state

  }

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

