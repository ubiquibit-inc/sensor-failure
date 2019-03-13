package com.ubiquibit.buoy

import java.io.File

import com.ubiquibit.FakeFile
import com.ubiquibit.buoy.StationId.makeStationId

class FakeFileReckoning extends FileReckoning {

  override val stationIds: Seq[StationId] = List("abcdefg", "bcdefg").map(makeStationId)

  override val supportByStation: Map[StationId, Seq[BuoyData]] = Map(stationIds.head -> Seq(Ocean, Text), stationIds(1) -> Seq(Text))

  override def getFile(stationId: StationId, ofType: BuoyData): Option[File] = Some(new FakeFile(stationId.toString))

}
