package com.ubiquibit.buoy.jobs

import com.ubiquibit.buoy.{BuoyData, StationId}

trait QueryNamer extends Serializable {
  def name(stationId: StationId, buoyData: BuoyData, qualifier: Option[String]): String = {
    if (qualifier.isDefined) s"$stationId-$buoyData-${qualifier.get}"
    else s"$stationId-$buoyData-records"
  }
}
