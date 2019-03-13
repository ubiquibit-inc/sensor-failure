package com.ubiquibit

import com.ubiquibit.buoy._

class FakeStationRepository extends StationRepository {

  var initCount = 0

  override def initStations(): Unit = {
    initCount = initCount + 1
  }

  var readResponse: Seq[StationInfo] = Nil

  var readCount = 0

  override def readStations(): Seq[StationInfo] = {
    readCount = readCount + 1
    readResponse
  }

  override def updateImportStatus(stationId: StationId, buoyData: BuoyData, importStatus: ImportStatus): Option[ImportStatus] = throw new NullPointerException("updateImportStatus")

  override def getImportStatus(stationId: StationId, buoyData: BuoyData): Option[ImportStatus] = throw new NullPointerException("getImportStatus")

  var deleteCount = 0

  override private[ubiquibit] def deleteStations(): Unit = {
    deleteCount = deleteCount + 1
  }

}
