package com.ubiquibit.buoy

import com.ubiquibit.TimeHelper
import org.scalatest.FunSpec
import org.scalatest.words.ShouldVerb

class StationInfoSpec extends FunSpec with ShouldVerb{

  private val statId0 = StationId.makeStationId("asdb")
  private val statId1 = StationId.makeStationId("zyzuser")

  val simple = StationInfo(statId0, 123)

  val withFeeds = StationInfo(statId1, 234, TimeHelper.epochTimeZeroUTC().toString, Map(Rain -> DONE, Ocean -> READY))
  private val withFeedsAsMap = Map[String, String](
    StationInfo.stationIdKey -> statId1.toString,
    StationInfo.reportFrequencyKey -> withFeeds.reportFrequencyMinutes.toString,
    Rain.toString -> DONE.toString, Ocean.toString -> READY.toString
  )

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

    it("instantiate with valueOf") {

      val result: StationInfo = StationInfo.valueOf(withFeedsAsMap).get

      assert(result === withFeeds)

    }
  }

}
