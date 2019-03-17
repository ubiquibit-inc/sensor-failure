package com.ubiquibit.buoy.jobs

import com.ubiquibit.{FakeSpark, FakeStationRepository, Spark}
import com.ubiquibit.buoy.{FakeFileReckoning, StationRepository}
import org.scalatest.{BeforeAndAfter, FunSpec}

class InitKafkaSpec extends FunSpec with BeforeAndAfter {

  val stationRepository: StationRepository = new FakeStationRepository
  private val fakeRepo = stationRepository.asInstanceOf[FakeStationRepository]
  val spark: Spark = new FakeSpark
  val fileReckoning = new FakeFileReckoning

  val instance = new InitKafkaImpl(env = this)

  after {
    fakeRepo.reset
  }

  describe("InitKafka should") {

    it("ask repo for StationInfo") {

      instance.run()

      assert(fakeRepo.readStationsCount == 1)

    }

  }

}