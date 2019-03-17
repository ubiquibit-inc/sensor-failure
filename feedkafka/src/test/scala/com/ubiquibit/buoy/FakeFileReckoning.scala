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
  var fakeFeeds: Map[StationId, Seq[BuoyData]] = Map()

  override def feeds(): collection.Map[StationId, Seq[BuoyData]] = {
    feedCount = feedCount + 1
    fakeFeeds
  }

  var fakeStationInfo: Seq[StationInfo] = Seq()
  var stationInfoCount = 0

  override def stationInfo(): Seq[StationInfo] = {
    stationInfoCount = stationInfoCount + 1
    fakeStationInfo
  }

  var pairCount = 0

  override def pairs(): List[(StationId, BuoyData)] = {
    pairCount = pairCount + 1
    List()
  }

  override def getFile(stationId: StationId, ofType: BuoyData): Option[File] = Some(new FakeFile(stationId.toString))
}
