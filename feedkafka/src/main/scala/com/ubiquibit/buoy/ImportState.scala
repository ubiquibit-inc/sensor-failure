package com.ubiquibit.buoy

sealed abstract class ImportState(desc: String)

object ImportState{
  val values: Set[ImportState] = Set(READY, WORKING, ERROR, DONE)
}

case object READY extends ImportState("ready")
case object WORKING extends ImportState("working")
case object ERROR extends ImportState("error")
case object DONE extends ImportState("done")
case object UNSUPPORTED extends ImportState("unsupported")
