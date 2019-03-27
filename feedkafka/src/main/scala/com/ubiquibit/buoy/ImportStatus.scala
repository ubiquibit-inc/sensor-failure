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
case object READY extends ImportStatus("ready") // downloaded to the load directory
case object WORKING extends ImportStatus("working") // importing into Kafka
case object DONE extends ImportStatus("done") // imported to kafka from load directory
case object LIVE extends ImportStatus("realtime") // serving directly from NDBC

// wolves & rain clouds
case object ERROR extends ImportStatus("error")
case object UNSUPPORTED extends ImportStatus("unsupported")
