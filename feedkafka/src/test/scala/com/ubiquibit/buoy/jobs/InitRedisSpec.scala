package com.ubiquibit.buoy.jobs

import com.ubiquibit.{StationRepository, Wiring}
import org.scalatest.FunSpec

class InitRedisSpec extends FunSpec{

  val instance: InitRedis = Wiring.initRedis
  val repo: StationRepository = Wiring.stationRepository

  def fixture =
    new {
      repo.deleteStations()
    }

  describe("InitRedis"){
    it("should start from a blank slate"){
      assert(repo.readStations().length === 0 )
    }
  }

}
