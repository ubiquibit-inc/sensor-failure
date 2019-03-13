package com.ubiquibit.buoy.jobs

import com.ubiquibit.buoy._
import org.scalatest.FunSpec

class InitRedisSpec extends FunSpec {

  val stationRepository: StationRepository = new FakeStationRepository

  private val fakeRepo: FakeStationRepository = stationRepository.asInstanceOf[FakeStationRepository]

  val fileReckoning: FileReckoning = new FakeFileReckoning()

  val instance: InitRedis = new InitRedisImpl(this)

  def reset = {
    fakeRepo.readResponse = Nil
    fakeRepo.readCount = 0
    fakeRepo.deleteCount = 0
    fakeRepo.initCount = 0
  }

  describe("InitRedis should") {

    it("do not too much when # stations in redis and # stations on disk is the same") {

      fakeRepo.readResponse = Seq(StationInfo("abcd", 23.toString), StationInfo("xyzpdq", 42.toString))
      instance.run()

      reset

    }

    it("initialize if # stations in redis is < # stations on disk") {

      // we should be able to chain return values, but I'm not going to write it right now...
      fakeRepo.readResponse = Seq[StationInfo]()
      // fileReckoning.stationIds == 2, so 0 < 2

      instance.run()

      assert(fakeRepo.readCount === 2)
      assert(fakeRepo.initCount === 2)

      reset

    }

  }

}

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
