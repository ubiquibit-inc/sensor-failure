package com.ubiquibit.buoy.jobs

import com.ubiquibit.Wiring
import com.ubiquibit.buoy.StationRepository
import org.scalatest.FunSpec

class InitRedisSpec extends FunSpec{

  val instance: InitRedis = Wiring.initRedis
  val repo: StationRepository = Wiring.stationRepository

  def fixture =
    new {
      repo.deleteStations()
    }

  describe("InitRedis should"){
    it("start from a blank slate"){
      assert(repo.readStations().length === 0 )
    }
  }

}
