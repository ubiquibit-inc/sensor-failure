package com.ubiquibit

import java.time.{LocalDateTime, ZoneOffset}

object TimeHelper {

  /**
    * The source weather stations report in UTC, so we use it too.
    */
  val defaultOffset: ZoneOffset = ZoneOffset.UTC

  def now(): LocalDateTime = {
    LocalDateTime.now(defaultOffset)
  }

  /**
    * @return epoch time zero
    */
  def epochTimeZeroUTC() : LocalDateTime = {
    LocalDateTime.ofEpochSecond(0, 0, defaultOffset)
  }

}
