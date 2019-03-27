package com.ubiquibit.buoy.jobs

import com.ubiquibit.buoy.{BuoyFeed, StationId}

trait QueryNamer extends Serializable {
  def name(stationId: StationId, buoyData: BuoyFeed, qualifier: Option[String]): String = {
    if (qualifier.isDefined) s"$stationId-$buoyData-${qualifier.get}"
    else s"$stationId-$buoyData-records"
  }
}
