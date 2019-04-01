package com.ubiquibit

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZoneOffset}
import java.util.Calendar

object TimeHelper {

  val defaultFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

  def from(s: String): Timestamp = {
    val date = defaultFormat.parse(s)
    new Timestamp(date.getTime)
  }

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
  def epochTimeZeroUTC(): LocalDateTime = {
    LocalDateTime.ofEpochSecond(0, 0, defaultOffset)
  }

  def epochTimeZeroTimestamp(): Timestamp = {
    Timestamp.valueOf(epochTimeZeroUTC())
  }

  private def toCal(ts: Timestamp): Calendar = {
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

  def randomNap(maxMillis: Int = 0): Unit = {
    if (maxMillis == 0) Thread.sleep(rand.nextInt(17750) + 250)
    else Thread.sleep(rand.nextInt(maxMillis))
  }

}
