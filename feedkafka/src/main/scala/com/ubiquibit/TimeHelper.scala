package com.ubiquibit

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}
import java.util.Calendar

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

  private def toCal(ts: Timestamp): Calendar ={
    val cal = Calendar.getInstance
    cal.setTime(ts)
    cal
  }

  def getDayOfMonth(ts: Timestamp) = {
    toCal(ts).get(Calendar.DAY_OF_MONTH)
  }

  def getYear(ts: Timestamp): Int = {
    toCal(ts).get(Calendar.YEAR)
  }

  def getMonth(ts: Timestamp): Int = {
    toCal(ts).get(Calendar.MONTH)
  }

  private val rand = scala.util.Random

  def randomNap(): Unit = {
    Thread.sleep(rand.nextInt(17750) + 250)
  }

}
