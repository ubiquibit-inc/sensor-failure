package com.ubiquibit.buoy

import com.ubiquibit.TimeHelper.epochTimeZeroUTC

/**
  * Summary information about weather stations, be they buoy or C-man
  * @param stationId @see https://www.ndbc.noaa.gov/staid.shtml
  * @param reportFrequencyMinutes calculated frequency of reported obx (not considering disruptions/failures)
  * @param lastReport time of last receipt
  */
case class StationInfo(stationId: String, reportFrequencyMinutes: String, lastReport: String = epochTimeZeroUTC().toString)