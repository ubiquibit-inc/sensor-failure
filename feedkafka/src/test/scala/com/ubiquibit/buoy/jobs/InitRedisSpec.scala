package com.ubiquibit.buoy.jobs

import org.scalatest.FunSpecLike

class InitRedisSpec extends InitRedis with FunSpecLike{

  def fixture =
    new {
      deleteStations()
    }

  describe("InitRedis"){
    it("should start from a blank slate"){
      assert(readStations().length === 0 )
    }
  }

}
