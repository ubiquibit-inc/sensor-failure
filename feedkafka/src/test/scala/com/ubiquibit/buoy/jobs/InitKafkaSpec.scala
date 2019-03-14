package com.ubiquibit.buoy.jobs

import com.ubiquibit.{FakeSpark, FakeStationRepository, Spark}
import com.ubiquibit.buoy.{FakeFileReckoning, StationRepository}
import org.scalatest.{BeforeAndAfter, FunSpec}

class InitKafkaSpec extends FunSpec with BeforeAndAfter {

  val stationRepository: StationRepository = new FakeStationRepository
  val fakeRepo = stationRepository.asInstanceOf[FakeStationRepository]
  val spark: Spark = new FakeSpark
  val fileReckoning = new FakeFileReckoning

  val instance = new InitKafkaImpl(env = this)

  after {
    fakeRepo.readResponse = Seq()
  }

  describe("InitKafka should") {

    it("asks repo for StationInfo") {

      instance.run()
      assert(fakeRepo.readCount == 1)

    }

  }

}