package com.ubiquibit.buoy

import java.sql.Timestamp

sealed abstract class WxRecord(eventTime: Timestamp)

case class TextRecord(eventTime: Timestamp, windDirection: Float, windSpeed: Float, gustSpeed: Float
                      , waveHeight: Float, dominantWavePeriod: Float, averageWavePeriod: Float, mWaveDirection: Float, seaLevelPressure: Float
                      , airTemp: Float, waterSurfaceTemp: Float, dewPointTemp: Float, visibility: Float, pressureTendency: Float, tide: Float) extends WxRecord(eventTime)

object WxRecord {

}