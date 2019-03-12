package com.ubiquibit.buoy

import com.ubiquibit.TimeHelper.epochTimeZeroUTC

case class StationInfo(stationId: String, reportFrequencyMinutes: String, lastReport: Any = epochTimeZeroUTC())