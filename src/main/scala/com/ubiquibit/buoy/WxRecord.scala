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

package com.ubiquibit.buoy

import java.sql.Timestamp

import com.ubiquibit.buoy.serialize.DefSer


sealed abstract class WxRecord(eventTime: Timestamp, lineLength: Int, stationId: String) extends Serializable {

  def valueOf(): Array[Byte] = DefSer.serialize(this)

}

case class TextRecord(eventTime: Timestamp, lineLength: Int, stationId: String, windDirection: Float, windSpeed: Float, gustSpeed: Float
                      , waveHeight: Float, dominantWavePeriod: Float, averageWavePeriod: Float, mWaveDirection: Float, seaLevelPressure: Float
                      , airTemp: Float, waterSurfaceTemp: Float, dewPointTemp: Float, visibility: Float, pressureTendency: Float, tide: Float) extends WxRecord(eventTime, lineLength, stationId)
