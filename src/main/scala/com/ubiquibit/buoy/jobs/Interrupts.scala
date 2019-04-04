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
  *
  * Provides information about a batch of [[TextRecord]]s: Whether each one, in eventTime sequence, has
  * had a sensor channel (e.g. windDirection) stop reporting or come online again.
  *
  * Properly used (by [[StationInterrupts]]), instances of this class carry with them a number of records.
  * The first [[StationInterrupts.numRecords]] [[TextRecord]]s fall into the "analysis window". That is,
  * they are checked vs their immediate predecessor for sensor channel interrupts and channel restorations.
  *
  * In addition, a trailing log of additional [[TextRecord]]s is supplied (for ML-purposes). These
  * "backlog window" records do not have interrupt and restorations calculated.
  *
  * Be advised that records are in a [[Map]], and therefore are returned in arbitrary order.
  *
  * For a given record-in-time, each interrupted channel shows up in the first [[Set]]. Channels that
  * come online again at that time are recorded in the second [[Set]].
  *
  */
case class Interrupts(var stationId: String, var records: Map[TextRecord, (Set[String], Set[String])]) {

  private val processRecordsCnt = StationInterrupts.numRecords
  import StationInterrupts._

  def isInterrupted: Boolean = records.exists(_._2._1.nonEmpty)

  def isOnlineAgain: Boolean = records.exists(_._2._2.nonEmpty)

  /**
    * @return unordered results from the analysis window
    */
  def inWindow(): Interrupts = {
    Interrupts(stationId, records.filterKeys(k=> analysisWindowKeys.contains(k)))
  }

  def inBacklog(): Interrupts = {
    Interrupts(stationId, records.filterKeys(k=> !analysisWindowKeys.contains(k)))
  }

  private val analysisWindowKeys = records.keys.toList.sortWith(sortRecords).dropRight(records.size - processRecordsCnt)

}


