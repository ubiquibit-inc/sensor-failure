package com.ubiquibit.buoy

/**
  * @see https://www.ndbc.noaa.gov/staid.shtml
  */
sealed abstract class StationId(prefix: String, id: String){
  override def toString = s"$prefix$id"
}

case class BuoyId(prefix: Int, id: String) extends StationId(prefix.toString, id)

case class CmanId(prefix: String, id: String) extends StationId(prefix, id)

/** The rest may go bye bye

  * This is the two-digit code for WMO oceanic regions.

sealed abstract class WmoRegion(code: Int){
  def codeStr(): String = code.toString
}

case object Pacific_SouthAmerica extends WmoRegion(32)

case object Atlantic_US_SouthEast extends WmoRegion(41)

case object Gulf_of_Mexico extends WmoRegion(42)

case object Atlantic_US_NorthOfNC extends WmoRegion(44)

case object GreatLakes extends WmoRegion(45)

case object Pacific_US extends WmoRegion(46)

case object Pacific_Hawaiian_Islands extends WmoRegion(51)

case object Pacific_Guam extends WmoRegion(52)

case class OceanStationId(region: WmoRegion, id: String) extends StationId(prefix = region.codeStr(), id)
*/