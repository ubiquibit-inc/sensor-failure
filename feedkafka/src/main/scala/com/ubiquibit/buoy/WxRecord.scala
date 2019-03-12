package com.ubiquibit.buoy

import java.sql.Timestamp

sealed abstract class WxRecord(eventTime: Timestamp)

case class TextRecord(eventTime: Timestamp, windDirection: Float, windSpeed: Float, gustSpeed: Float
                      , waveHeight: Float, dpd: Float, apd: Float, mwd: Float, pres: Float
                      , atmp: Float, wtmp: Float, dewp: Float, vis: Float, ptdy: Float, tide: Float) extends WxRecord(eventTime)

object WxRecord {

}