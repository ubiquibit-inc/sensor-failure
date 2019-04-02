/*
 * Copyright (c) 2019.
 *
 * This work, (c) by jason@ubiquibit.com
 *
 * This work is licensed under a
 * Creative Commons Attribution-ShareAlike 4.0 International License.
 *
 * You should have received a copy of the license along with this
 * work.  If not, see <http://creativecommons.org/licenses/by-sa/4.0/>.
 *
 */

package com.ubiquibit

import java.sql.Timestamp

import com.ubiquibit.buoy.TextRecord

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * This work (c) by jason@ubiquibit.com
  *
  * This work is licensed under a
  * Creative Commons Attribution-ShareAlike 4.0 International License.
  *
  * You should have received a copy of the license along with this
  *   work.  If not, see <http://creativecommons.org/licenses/by-sa/4.0/>.
  */
trait RandomData {

  val rand: scala.util.Random = new Random

  def f: Float = math.abs(rand.nextFloat())

  def nf: Float = Float.NaN

  def i: Int = math.abs(rand.nextInt)

  def s: String = s"str$i"

  def ts: Timestamp = {
    val stamp = new Timestamp(System.currentTimeMillis())
    Thread.sleep(10)
    stamp
  }

  def rec(stationId: Option[String] = Some(s)): TextRecord =
    TextRecord(ts, i, stationId.getOrElse(s), f, f, f, f, f, f, f, f, f, f, f, f, f, f)

  def testRecords(len: Option[Int] = Some(32), stationId: Option[String] = Some(s)): Seq[TextRecord] = {
    val myLen = len.getOrElse(32)
    val ab = ArrayBuffer.fill(myLen){null.asInstanceOf[TextRecord]}
    (0 until myLen).foreach { x =>
      ab(x) = rec(stationId)
    }
    ab
  }

}
