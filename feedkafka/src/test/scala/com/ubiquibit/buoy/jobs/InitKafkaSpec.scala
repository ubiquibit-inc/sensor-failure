package com.ubiquibit.buoy.jobs

import com.ubiquibit.buoy.StationRepository
import org.scalatest.FunSpec

class InitKafkaSpec extends FunSpec {

  val repo: StationRepository = ???

  describe("InitKafka should") {

    it("ask Redis for a file in the ready state") {

      val stations = repo.readStations()
    }

  }

}