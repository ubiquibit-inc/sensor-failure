package com.ubiquibit.buoy.jobs

import com.ubiquibit.{FakeStationRepository, TimeHelper}
import com.ubiquibit.buoy.StationId
import org.apache.spark.sql.Row
import org.scalatest.{BeforeAndAfter, FunSpec}

class LastReportUpdaterSpec extends FunSpec with BeforeAndAfter{

  private val repo: FakeStationRepository = new FakeStationRepository
  private val stationId: StationId = StationId.makeStationId("zyayysdf")
  private val instance: LastReportUpdater = new LastReportUpdater(stationId) //, repo)

  after{
    repo.reset
  }

  describe("LastReportUpdater") {

    it("call") {

      val row: Row = new Row {
        override def length: Int = 1
        override def get(i: Int): Any = TimeHelper.epochTimeZeroTimestamp().getTime
        override def copy(): Row = this
      }

      assert( repo.updateLastReportCount === 0)

      instance.process(row)

      assert( repo.updateLastReportCount === 0)

    }
  }

}
