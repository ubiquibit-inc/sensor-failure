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

  /** A semaphore, so that we may simulate streaming (in absence of streaming) */
  def effectiveNow(): LocalDateTime = ???

  /**
    * @return epoch time zero
    */
  def epochTimeZeroUTC() : LocalDateTime = {
    LocalDateTime.ofEpochSecond(0, 0, defaultOffset)
  }

}
