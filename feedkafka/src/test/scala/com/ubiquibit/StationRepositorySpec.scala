package com.ubiquibit

import com.ubiquibit.buoy.{BuoyData, READY, StationId, UNSUPPORTED}
import org.scalatest.FunSpec

import scala.util.Try

class StationRepositorySpec extends FunSpec{

  // TODO test wiring
  val instance: StationRepository = Wiring.stationRepository

  describe("StationRepository should") {
    it("be implemented") {
      assert(1 === 2)
    }
    it("should update import status") {
      assert(Try(instance.updateImportStatus(StationId.makeStationId("abababa"), BuoyData.values.head, READY)).isSuccess)
    }
    it("should return import status") {
      val result = instance.getImportStatus(StationId.makeStationId("aadbkasjdgbj"), BuoyData.values.head)
      assert(result === UNSUPPORTED)
    }

  }
}
