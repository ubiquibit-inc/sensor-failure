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

package com.ubiquibit.buoy

import java.io.File

import com.ubiquibit.FakeFile
import com.ubiquibit.buoy.StationId.makeStationId

class FakeFileReckoning extends FileReckoning {

  def reset: Unit = {
    fakeStationInfo = Seq()
    fakeStationIds = Seq()
    fakeFeeds = Map()

    stationInfoCount = 0
    stationIdCount = 0
    pairCount = 0
    feedCount = 0

  }

  var stationIdCount = 0
  var fakeStationIds: Seq[StationId] = Seq()

  override def stationIds(): Seq[StationId] = {
    stationIdCount = stationIdCount + 1
    fakeStationIds
  }

  var feedCount = 0
  var fakeFeeds: Map[StationId, Seq[WxFeed]] = Map()

  override def feeds(): collection.Map[StationId, Seq[WxFeed]] = {
    feedCount = feedCount + 1
    fakeFeeds
  }

  var fakeStationInfo: Seq[WxStation] = Seq()
  var stationInfoCount = 0

  override def stationInfo(): Seq[WxStation] = {
    stationInfoCount = stationInfoCount + 1
    fakeStationInfo
  }

  var pairCount = 0

  override def pairs(): List[(StationId, WxFeed)] = {
    pairCount = pairCount + 1
    List()
  }

  override def getFile(stationId: StationId, ofType: WxFeed): Option[File] = Some(new FakeFile(stationId.toString))
}
