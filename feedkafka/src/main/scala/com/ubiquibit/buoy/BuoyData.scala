package com.ubiquibit.buoy

import java.io.File
import java.time.LocalDateTime

/**
  * The "realtime" buoy data formats are described
  *
  * @see https://dods.ndbc.noaa.gov/ and
  * @see https://www.ndbc.noaa.gov/rt_data_access.shtml and
  * @see https://www.ndbc.noaa.gov/measdes.shtml
  * @see https://www.ndbc.noaa.gov/staid.shtml
  */
sealed abstract class BuoyData(extension: String){
  def same(file: File): Boolean = file.getName.endsWith(extension)
}

object BuoyData {
  val values: Set[BuoyData] = Set(Adcp, Adcp2, Cwind, Dart, DataSpec, Drift, Hkp, Ocean, Rain, Spec, Srad, Supl, Swr1, Swr2, Text)
}

case object Adcp extends BuoyData("adcp")
case object Adcp2 extends BuoyData("adcp2")
case object Cwind extends BuoyData("cwind")
case object Dart extends BuoyData("dart")
case object DataSpec extends BuoyData("data_spec")
case object Drift extends BuoyData("drift")
case object Hkp extends BuoyData("hkp")
case object Ocean extends BuoyData("ocean")
case object Rain extends BuoyData("rain")
case object Spec extends BuoyData("spec")
case object Srad extends BuoyData("srad")
case object Supl extends BuoyData("supl")
case object Swdir extends BuoyData("swdir")
case object Swr1 extends BuoyData("swr1")
case object Swr2 extends BuoyData("swr2")
case object Text extends BuoyData("txt")

sealed abstract class Format( eventTime: LocalDateTime )

//case object TextFmt extends Format()