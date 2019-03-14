package com.ubiquibit

import com.ubiquibit.buoy.{StationId, Text, TextRecord, WxRecord}
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.scalatest.{BeforeAndAfter, FunSpec}

import scala.util.Random

class ProducersSpec extends FunSpec with BeforeAndAfter {

  private val testCallback = new TestCallback
  private val testRecord = TextRecord(TimeHelper.epochTimeZeroTimestamp(), rF(), rF(), rF(), rF(), rF(), rF(), rF(), rF(), rF(), rF(), rF(), rF(), rF(), rF())

  implicit def stationRepository = new FakeStationRepository

  val instance:Producer[WxRecord] = Producers.of(StationId.makeStationId("test"), Text)(stationRepository)

  def rF(): Float = {
    Random.nextFloat()
  }

  after{
    testCallback.calledBack = false
  }

  describe("Producers should") {
    it("give me a Producer that can send a message to the test topic") {
      instance.send(testRecord, testCallback)

      TimeHelper.randomNap(500)
      assert(testCallback.calledBack)
    }
  }

  class TestCallback extends Callback {
    var calledBack = false

    override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
      if (e != null) {
        println("Error on callback")
        e.printStackTrace()
      }
      else calledBack = true
    }
  }

}
