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

import java.io.File

/**
  * The "realtime" buoy data formats are described
  *
  * @see https://dods.ndbc.noaa.gov/ and
  * @see https://www.ndbc.noaa.gov/rt_data_access.shtml and
  * @see https://www.ndbc.noaa.gov/measdes.shtml
  * @see https://www.ndbc.noaa.gov/staid.shtml
  */
sealed abstract class WxFeed(extension: String){
  def same(file: File): Boolean = file.getName.endsWith(extension)
  def ext: String = extension
  override def toString = ext.toUpperCase
}

object WxFeed {
  val values: Set[WxFeed] = Set(Adcp, Adcp2, Cwind, Dart, DataSpec, Drift, Hkp, Ocean, Rain, Spec, Srad, Supl, Swr1, Swr2, Text)
  def valueOf(str: String) : Option[WxFeed] = values.find(_.ext.equalsIgnoreCase(str))
}

case object Adcp extends WxFeed("adcp")
case object Adcp2 extends WxFeed("adcp2")
case object Cwind extends WxFeed("cwind")
case object Dart extends WxFeed("dart")
case object DataSpec extends WxFeed("data_spec")
case object Drift extends WxFeed("drift")
case object Hkp extends WxFeed("hkp")
case object Ocean extends WxFeed("ocean")
case object Rain extends WxFeed("rain")
case object Spec extends WxFeed("spec")
case object Srad extends WxFeed("srad")
case object Supl extends WxFeed("supl")
case object Swdir extends WxFeed("swdir")
case object Swr1 extends WxFeed("swr1")
case object Swr2 extends WxFeed("swr2")
case object Text extends WxFeed("txt")
