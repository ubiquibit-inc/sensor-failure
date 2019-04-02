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

package com.ubiquibit.buoy.jobs

import com.ubiquibit.buoy.TextRecord

/**
  * This work, (c) by jason@ubiquibit.com
  *
  * This work is licensed under a
  * Creative Commons Attribution-ShareAlike 4.0 International License.
  *
  * You should have received a copy of the license along with this
  *   work.  If not, see <http://creativecommons.org/licenses/by-sa/4.0/>.
  *
  * @param stationId event source
  * @param records map from individual event to (channel interrupts, re-activation events) on that tick
  */
case class Interrupts(var stationId: String, var records: Map[TextRecord, (Set[String], Set[String])]) {

  private val processRecordsCnt = StationInterrupts.numRecords
  import StationInterrupts._

  def isInterrupted: Boolean = records.exists(_._2._1.nonEmpty)

  def isOnlineAgain: Boolean = records.exists(_._2._2.nonEmpty)

  def inWindow(): Interrupts = {
    val keepers = records.keys.toList.sortWith(sortRecords).dropRight(records.size - processRecordsCnt)
    Interrupts(stationId, records.filterKeys(k=> keepers.contains(k)))
  }

}


