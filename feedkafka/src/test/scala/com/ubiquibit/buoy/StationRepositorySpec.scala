package com.ubiquibit.buoy

import com.redis.RedisClient
import com.ubiquibit._
import org.scalatest.{BeforeAndAfter, FunSpec}
import com.ubiquibit.buoy.StationId.makeStationId

import scala.collection.immutable.HashMap

class StationRepositorySpec extends FunSpec with BeforeAndAfter {

  /** manual mocking **/
  val fileReckoning: FileReckoning = new FakeFileReckoning

  val client: RedisClient = new FakeRedisClient
  private val fakeClient = client.asInstanceOf[FakeRedisClient]
  val redis: Redis = new FakeRedis(client)

  val instance: StationRepository = new StationRepositorImpl(this)

  after {
    fakeClient.delCount = 0
    fakeClient.getCount = 0
    fakeClient.setCount = 0
    fakeClient.hmgetResult = Seq()
  }

  private val station0 = fileReckoning.stationIds.head
  private val station1 = fileReckoning.stationIds(1)
  private val station0type0 = fileReckoning.supportByStation(station0).head
  private val station0type1 = fileReckoning.supportByStation(station0).tail.head
  private val station1type0 = fileReckoning.supportByStation(station1).head

  private val station1ReadyResponse = HashMap[Any, String](station1type0.toString.toUpperCase -> READY.toString)

  describe("StationRepository should") {

    it("delete stations from redis") {

      instance.deleteStations()
      assert(fakeClient.delCount === fileReckoning.stationIds.length)

    }

    it("report import status") {

      val mt = instance.getImportStatus(makeStationId("aadbkasjdgbj"), BuoyData.values.head)
      assert(mt === None)
      assert(fakeClient.getCount === 1)

      fakeClient.hmgetResult = Seq(HashMap(station0type0.toString.toUpperCase -> READY.toString, station0type1.toString.toUpperCase -> READY.toString))
      val result0 = instance.getImportStatus(station0, station0type0)
      assert(result0.isDefined)
      assert(fakeClient.getCount === 2)
      val status = result0.get
      assert(status === READY)

      fakeClient.hmgetResult = Seq(station1ReadyResponse)
      val result1 = instance.getImportStatus(station1, Ocean)
      assert(result1 === None)
      assert(fakeClient.getCount == 3)

    }

    it("update import status") {

      val result0 = instance.updateImportStatus(makeStationId("abababa"), BuoyData.values.head, READY)
      assert(fakeClient.getCount === 1) // checking for station existence in Redis
      assert(fakeClient.setCount === 0)
      assert(result0 === None)

      fakeClient.hmgetResult = Seq(station1ReadyResponse)
      val result1 = instance.updateImportStatus(station1, station1type0, DONE)
      assert(fakeClient.getCount === 2)
      assert(fakeClient.setCount === 1)

    }

    it("reads station info from redis") {

      val s0 = StationInfo(station1, 0, TimeHelper.epochTimeZeroUTC().toString, Map(Adcp -> READY, Adcp2 -> ERROR))
      val s1 = StationInfo(station0, 0, TimeHelper.epochTimeZeroUTC().toString, Map(Text -> DONE, Hkp -> READY))

      fakeClient.hmgetResult = Seq(s0.toMap.asInstanceOf[Map[Any, String]], s1.toMap.asInstanceOf[Map[Any, String]])

      val result0 = instance.readStations()

      assert(result0.length == 2)

    }

  }

}