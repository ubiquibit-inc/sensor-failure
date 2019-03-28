package com.ubiquibit.buoy.jobs

import java.sql.Timestamp

import com.ubiquibit.buoy.TextRecord
import org.apache.spark.sql.streaming.GroupState

import scala.collection.mutable.ArrayBuffer

/**
  * Arbitrary Stateful Processing for [[com.ubiquibit.buoy.WxStation]]
  *
  * @see https://databricks.com/blog/2017/10/17/arbitrary-stateful-processing-in-apache-sparks-structured-streaming.html
  */
case class StationInterrupts(var stationId: String, var lastRecord: TextRecord, var interrupts: Set[String])

object StationInterrupts {

  def updateInterruptsWithEvent(state: StationInterrupts, input: TextRecord): StationInterrupts = {

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
  private val year2169 = new Timestamp(6284160000000L) // 2169-02-19 02:00:00.0
  private val defaultRecord = TextRecord(year1969, -1, "", Float.NaN, Float.NaN, Float.NaN, Float.NaN,Float.NaN, Float.NaN,Float.NaN, Float.NaN,Float.NaN, Float.NaN,Float.NaN, Float.NaN,Float.NaN, Float.NaN)

  val defaultState: StationInterrupts = StationInterrupts(
    stationId = "",
    lastRecord = defaultRecord,
    interrupts = Set[String]()
//    ,
//    start = year1969,
//    end = year1969
  )

  def updateAcrossEvents(stationId: String,
                         inputs: Iterator[TextRecord],
                         oldState: GroupState[StationInterrupts]): StationInterrupts = {

    var state: StationInterrupts = if( oldState.exists ) oldState.get else defaultState

    for( input <- inputs ){
      state = updateInterruptsWithEvent(state, input)
      oldState.update(state)
    }

    state

  }

  private def onlineAgain(previousRecord: TextRecord, currentRecord: TextRecord): Set[String] = {
    val result = ArrayBuffer.empty[String]

    def addOne(str: String) = {
      result += str
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

    result.toSet

  }

  private def interrupts(previousRecord: TextRecord, currentRecord: TextRecord): Set[String] = {

    val result = ArrayBuffer.empty[String]

    def addOne(str: String) = {
      result += str
    }

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

    result.toSet
  }

  def main(args:Array[String]): Unit = {
    println("start: " + year1969)
    println("end: " + year2169)
  }

}

