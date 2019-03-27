package com.ubiquibit.buoy

trait SupportedFeeds {

  def supported: List[BuoyFeed] = List(Text) // TODO load from Config instead, perhaps dynamically

}
