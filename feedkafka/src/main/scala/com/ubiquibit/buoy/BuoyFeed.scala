package com.ubiquibit.buoy

import java.io.File

/**
  * The "realtime" buoy data formats are described
  *
  * @see https://dods.ndbc.noaa.gov/ and
  * @see https://www.ndbc.noaa.gov/rt_data_access.shtml and
  * @see https://www.ndbc.noaa.gov/measdes.shtml
  * @see https://www.ndbc.noaa.gov/staid.shtml
  */
sealed abstract class BuoyFeed(extension: String){
  def same(file: File): Boolean = file.getName.endsWith(extension)
  def ext: String = extension
  override def toString = ext.toUpperCase
}

object BuoyFeed {
  val values: Set[BuoyFeed] = Set(Adcp, Adcp2, Cwind, Dart, DataSpec, Drift, Hkp, Ocean, Rain, Spec, Srad, Supl, Swr1, Swr2, Text)
  def valueOf(str: String) : Option[BuoyFeed] = values.find(_.ext.equalsIgnoreCase(str))
}

case object Adcp extends BuoyFeed("adcp")
case object Adcp2 extends BuoyFeed("adcp2")
case object Cwind extends BuoyFeed("cwind")
case object Dart extends BuoyFeed("dart")
case object DataSpec extends BuoyFeed("data_spec")
case object Drift extends BuoyFeed("drift")
case object Hkp extends BuoyFeed("hkp")
case object Ocean extends BuoyFeed("ocean")
case object Rain extends BuoyFeed("rain")
case object Spec extends BuoyFeed("spec")
case object Srad extends BuoyFeed("srad")
case object Supl extends BuoyFeed("supl")
case object Swdir extends BuoyFeed("swdir")
case object Swr1 extends BuoyFeed("swr1")
case object Swr2 extends BuoyFeed("swr2")
case object Text extends BuoyFeed("txt")
