package com.ubiquibit.buoy

import com.ubiquibit.TimeHelper
import org.scalatest.FunSpec

class StationInfoSpec extends FunSpec {

  val statId0 = StationId.makeStationId("asdb")
  val statId1 = StationId.makeStationId("zyzuser")

  val simple = StationInfo(statId0, 123)
  val withFeeds = StationInfo(statId1, 234, TimeHelper.epochTimeZeroUTC().toString, Map(Rain -> DONE, Ocean -> READY))

  describe("StationInfo should") {

    it("map with toMap") {

      val s = simple.toMap
      assert(s(StationInfo.stationIdKey) === statId0.toString)
      assert(s(StationInfo.reportFrequencyKey) === "123")

      val t = withFeeds.toMap
      assert(t(StationInfo.stationIdKey) === statId1.toString)
      assert(t(StationInfo.reportFrequencyKey) === "234")
      assert(t(Rain.toString.toUpperCase) === DONE.toString.toUpperCase)
      assert(t(Ocean.toString.toUpperCase) === READY.toString.toUpperCase)

    }

    it("instatiate with valueOf") {

      val input = Map(StationInfo.stationIdKey -> "zyzuser", StationInfo.reportFrequencyKey -> "234", "RAIN" -> "DONE", "OCEAN" -> "READY")

      val result = StationInfo.valueOf(input)

      assert(result.isDefined)
      assert(result.get === withFeeds)

    }
  }

}
