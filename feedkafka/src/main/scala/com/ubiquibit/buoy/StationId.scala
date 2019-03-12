package com.ubiquibit.buoy

import scala.util.Try

/**
  * @see https://www.ndbc.noaa.gov/staid.shtml
  */
sealed abstract class StationId(prefix: String, id: String){
  override def toString = s"$prefix$id"
}

case class BuoyId(prefix: Int, id: String) extends StationId(prefix.toString, id)

case class CManId(prefix: String, id: String) extends StationId(prefix, id)

object StationId{

  /**
    * Take a filename and derive a StationId from it (polymorphic...)
    *
    * @param str some string
    * @return such a beast
    */
  def makeStationId(str: String): StationId = {
    def tryBuoyId(str: String): Try[StationId] = {
      val s1 = str.substring(0, 2)
      val s2 = str.substring(2)
      Try(BuoyId(s1.toInt, s2))
    }
    tryBuoyId(str) getOrElse CManId(str.substring(0, 2), str.substring(2))
  }

}