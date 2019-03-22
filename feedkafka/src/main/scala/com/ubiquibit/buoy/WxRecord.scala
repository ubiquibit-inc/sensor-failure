package com.ubiquibit.buoy

import java.sql.Timestamp

import com.ubiquibit.buoy.serialize.{DefSer}


sealed abstract class WxRecord(eventTime: Timestamp, lineLength: Int) extends Serializable {

  def valueOf(): Array[Byte] = DefSer.serialize(this)

}

case class TextRecord(eventTime: Timestamp, lineLength: Int, windDirection: Float, windSpeed: Float, gustSpeed: Float
                      , waveHeight: Float, dominantWavePeriod: Float, averageWavePeriod: Float, mWaveDirection: Float, seaLevelPressure: Float
                      , airTemp: Float, waterSurfaceTemp: Float, dewPointTemp: Float, visibility: Float, pressureTendency: Float, tide: Float) extends WxRecord(eventTime, lineLength)
