package com.ubiquibit.buoy.jobs

import com.ubiquibit.FakeStationRepository
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

      fakeRepo.readResponse = Seq(StationInfo(StationId.makeStationId("abcd"), 23), StationInfo(StationId.makeStationId("xyzpdq"), 42))
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