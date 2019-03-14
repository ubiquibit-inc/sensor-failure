package com.ubiquibit.buoy

import com.ubiquibit.TimeHelper.epochTimeZeroUTC

/**
  * Summary information about weather stations, be they buoy or C-man
  *
  * @param stationId              @see https://www.ndbc.noaa.gov/staid.shtml
  * @param reportFrequencyMinutes calculated frequency of reported obx (not considering disruptions/failures)
  * @param lastReport             time of last receipt
  */
case class StationInfo(stationId: StationId,
                       reportFrequencyMinutes: Int,
                       lastReport: String = epochTimeZeroUTC().toString,
                       feeds: Map[BuoyData, ImportStatus] = Map.empty) {

  def toMap: Map[String, String] = {
    Map(StationInfo.stationIdKey -> stationId.toString, StationInfo.reportFrequencyKey -> reportFrequencyMinutes.toString, StationInfo.lastReportKey -> lastReport) ++
      feeds.map { case (k, v) => (k.toString.toUpperCase, v.toString.toUpperCase) }
  }

}

object StationInfo {

  private[buoy] val stationIdKey: String = "stationId"
  private[buoy] val reportFrequencyKey: String = "reportFrequencyMinutes"
  private[buoy] val lastReportKey: String = "lastReport"

  def valueOf(map: Map[String, String]): Option[StationInfo] = {
    val stationId = map get stationIdKey
    val reportFreq = map get reportFrequencyKey
    val lastRpt = map get lastReportKey
    val feeds = map
      .filter((t) => BuoyData.valueOf(t._1).isDefined && ImportStatus.valueOf(t._2).isDefined)
      .map((t) => (BuoyData.valueOf(t._1).get, ImportStatus.valueOf(t._2).get))
    if (stationId.isDefined && reportFreq.isDefined ){
      var rpt = ""
      if( lastRpt.isEmpty ) rpt = epochTimeZeroUTC().toString
      else rpt = lastRpt.get
      Some(StationInfo(StationId.makeStationId(stationId.get), reportFreq.get.toInt, rpt, feeds))
    }
    else None
  }

}