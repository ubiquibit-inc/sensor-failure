package com.ubiquibit.buoy

import java.io.File

import com.redis.RedisClient
import com.ubiquibit.{FakeFile, FakeRedis, FakeRedisClient, Redis}
import org.scalatest.FunSpec
import com.ubiquibit.buoy.StationId.makeStationId

import scala.collection.immutable.HashMap

class StationRepositorySpec extends FunSpec {

  /** manual mocking **/
  val fileReckoning: FileReckoning = new FakeFileReckoning

  val client: RedisClient = new FakeRedisClient
  private val fakeClient = client.asInstanceOf[FakeRedisClient]
  val redis: Redis = new FakeRedis(client)

  val instance: StationRepository = new StationRepositorImpl(this)

  def reset: Unit = {
    fakeClient.delCount = 0
    fakeClient.getCount = 0
    fakeClient.setCount = 0
    fakeClient.hmgetResult = Map.empty
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

      reset

    }

    it("report import status") {

      val mt = instance.getImportStatus(makeStationId("aadbkasjdgbj"), BuoyData.values.head)
      assert(mt === None)
      assert(fakeClient.getCount === 1)

      fakeClient.hmgetResult = HashMap(station0type0.toString.toUpperCase -> READY.toString, station0type1.toString.toUpperCase -> READY.toString)
      val result0 = instance.getImportStatus(station0, station0type0)
      assert(result0.isDefined)
      assert(fakeClient.getCount === 2)
      val status = result0.get
      assert(status === READY)

      fakeClient.hmgetResult = station1ReadyResponse
      val result1 = instance.getImportStatus(station1, Ocean)
      assert(result1 === None)
      assert(fakeClient.getCount == 3)

      reset
    }

    it("update import status") {

      val result0 = instance.updateImportStatus(makeStationId("abababa"), BuoyData.values.head, READY)
      assert(fakeClient.getCount === 1) // checking for station existence in Redis
      assert(fakeClient.setCount === 0)
      assert(result0 === None)

      reset

      fakeClient.hmgetResult = station1ReadyResponse
      val result1 = instance.updateImportStatus(station1, station1type0, DONE)
      assert(fakeClient.getCount === 1)
      assert(fakeClient.setCount === 1)

      reset
    }

    it("reads station info from redis") {

      val result0 = instance.readStations()

      // TODO improve me (a lot)

    }

  }

}