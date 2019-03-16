package com.ubiquibit.buoy

sealed abstract class ImportStatus(description: String){
  def desc: String = description
  override def toString: String = desc.toUpperCase
}

/**
  * Weather data import status
  */
object ImportStatus{
  val values: Set[ImportStatus] = Set(READY, WORKING, ERROR, DONE)
  def valueOf(str: String): Option[ImportStatus] = values.find(_.desc.equalsIgnoreCase(str.trim()))
}

// bunnies & flowers
case object READY extends ImportStatus("ready")
case object WORKING extends ImportStatus("working")
case object DONE extends ImportStatus("done")

// wolves & rain clouds
case object ERROR extends ImportStatus("error")
case object UNSUPPORTED extends ImportStatus("unsupported")
