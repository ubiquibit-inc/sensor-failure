package com.ubiquibit.buoy

trait SupportedFeeds {

  def supported: List[BuoyData] = List(Text) // TODO load from Config instead, perhaps dynamically

}
