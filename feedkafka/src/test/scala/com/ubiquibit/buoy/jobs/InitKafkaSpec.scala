package com.ubiquibit.buoy.jobs

import com.ubiquibit.buoy.jobs.setup.InitKafkaImpl
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

  ignore("InitKafka should") {

    it("ask repo for StationInfo") {

      instance.run(None)

      assert(fakeRepo.readStationsCount == 1)

    }

  }

}