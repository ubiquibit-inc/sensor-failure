package com.ubiquibit.buoy.jobs

import java.sql.Timestamp

import com.ubiquibit.buoy.TextRecord
import org.scalatest.FunSpec

class RecordsSpec extends FunSpec {

  val testStationId = "inLife"

  def testRecord: TextRecord = {
    val tr = new TextRecord(new Timestamp(System.currentTimeMillis()), 0, testStationId, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F, 1F)
    Thread.sleep(2)
    tr
  }

  describe("Heap") {

    val testSize = 16
    val instance: Records = new Records(testSize)

    for (x <- 0 until testSize) {
      instance.push(testRecord)
    }

    it("add should:") {


      val testRec = testRecord
      instance.push(testRec)

      assert(instance.q.last === testRec)
      assert(instance.q.size === testSize)

    }

    it("recentRecs") {

      assert(2 === instance.recents().length)

      val recs = new Records(testSize)
      assert(0 === recs.recents().length)

      val rec0 = testRecord
      recs.push(rec0)
      assert(1 === recs.recents().length)

      val rec1 = testRecord
      recs.push(rec1)
      assert(rec1.eventTime after rec0.eventTime)

      val list = recs.recents().toList
      assert(list(1) === rec0)
      assert(list.head === rec1)

      assert(list.head.eventTime after list(1).eventTime)

    }
  }


}
