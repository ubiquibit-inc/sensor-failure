package com.ubiquibit

import com.ubiquibit.buoy._

class FakeStationRepository extends StationRepository {

  var readResponse: Seq[StationInfo] = Seq()

  var readStationsCount = 0

  override def readStations(): Seq[StationInfo] = {
    readStationsCount = readStationsCount + 1
    readResponse
  }

  override def updateImportStatus(stationId: StationId, buoyData: BuoyData, importStatus: ImportStatus): Option[ImportStatus] = throw new NullPointerException("updateImportStatus")

  override def getImportStatus(stationId: StationId, buoyData: BuoyData): Option[ImportStatus] = throw new NullPointerException("getImportStatus")

  var deleteCount = 0

  override def deleteStations(): Boolean = {
    deleteCount = deleteCount + 1
    true
  }

  override def readStation(stationId: StationId): Option[StationInfo] = ???

  var saveCount = 0

  override def saveStation(stationInfo: StationInfo): Option[StationId] = {
    saveCount = saveCount + 1
    Some(stationInfo.stationId)
  }

}
