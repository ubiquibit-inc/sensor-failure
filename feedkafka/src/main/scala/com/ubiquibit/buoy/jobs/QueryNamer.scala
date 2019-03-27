package com.ubiquibit.buoy.jobs

import com.ubiquibit.buoy.{WxFeed, StationId}

trait QueryNamer extends Serializable {
  def name(stationId: StationId, buoyData: WxFeed, qualifier: Option[String]): String = {
    if (qualifier.isDefined) s"$stationId-$buoyData-${qualifier.get}"
    else s"$stationId-$buoyData-records"
  }
}
