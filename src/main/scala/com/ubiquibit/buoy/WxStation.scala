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

import com.ubiquibit.TimeHelper.epochTimeZeroUTC

/**
  * Summary information about weather stations, be they buoy or C-man
  *
  * @param stationId              @see https://www.ndbc.noaa.gov/staid.shtml
  * @param reportFrequencyMinutes calculated frequency of reported obx (not considering disruptions/failures)
  * @param lastReport             time of last receipt
  */
case class WxStation(stationId: StationId,
                     reportFrequencyMinutes: Int,
                     lastReport: String = epochTimeZeroUTC().toString,
                     feeds: Map[WxFeed, WxFeedStatus] = Map.empty) {

  def toMap: Map[String, String] = {
    Map(WxStation.stationIdKey -> stationId.toString, WxStation.reportFrequencyKey -> reportFrequencyMinutes.toString, WxStation.lastReportKey -> lastReport) ++
      feeds.map { case (k, v) => (k.toString, v.toString) }
  }

  override def canEqual(that: Any): Boolean = true

  override def equals(obj: scala.Any): Boolean = true
}

object WxStation extends SupportedFeeds {

  private[buoy] val stationIdKey: String = "stationId"
  private[buoy] val reportFrequencyKey: String = "reportFrequencyMinutes"
  private[buoy] val lastReportKey: String = "lastReport"

  private val allFeeds: List[String] = WxFeed.values.map(v => v.toString).toList
  private val supportedFeeds: List[String] = supported.map(v => v.toString)
  private[buoy] val staticFields: List[String] = stationIdKey :: reportFrequencyKey :: lastReportKey :: Nil
  private[buoy] val fields: List[String] = staticFields ++ supportedFeeds // ++ allFeeds

  def valueOf(map: Map[String, String]): Option[WxStation] = {
    val stationId = map get stationIdKey
    val reportFreq = map get reportFrequencyKey
    val lastRpt = map get lastReportKey
    val feeds = map
      .filter((t) => WxFeed.valueOf(t._1).isDefined && WxFeedStatus.valueOf(t._2).isDefined)
      .map((t) => (WxFeed.valueOf(t._1).get, WxFeedStatus.valueOf(t._2).get))
    if (stationId.isDefined && reportFreq.isDefined) {
      var rpt = ""
      if (lastRpt.isEmpty) rpt = epochTimeZeroUTC().toString
      else rpt = lastRpt.get
      Some(WxStation(StationId.makeStationId(stationId.get), reportFreq.get.toInt, rpt, feeds))
    }
    else None
  }

}