package com.ubiquibit.buoy

trait SupportedFeeds {

  def supported: List[WxFeed] = List(Text) // TODO load from Config instead, perhaps dynamically

}
