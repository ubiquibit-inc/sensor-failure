package com.ubiquibit

import com.ubiquibit.buoy._
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.scalatest.{BeforeAndAfter, FunSpec, Tag}

import scala.util.Random

object FlakyFileTest extends Tag("FlakyFileTest")

class ProducersSpec extends FunSpec with BeforeAndAfter {

  private val testCallback = new TestCallback
  private val fakeLineLength = 23
  private val testRecord = TextRecord(TimeHelper.epochTimeZeroTimestamp(), fakeLineLength, rF(), rF(), rF(), rF(), rF(), rF(), rF(), rF(), rF(), rF(), rF(), rF(), rF(), rF())

  implicit def stationRepository: StationRepository = new FakeStationRepository

  def rF(): Float = {
    Random.nextFloat()
  }

  after {
    testCallback.calledBack = false
  }

  ignore("Producers should") {

    val instance: Producer[WxRecord] = Producers.of(StationId.makeStationId("test"), Text)(stationRepository)

    it("give me a Producer that can send a message to the test topic", FlakyFileTest) {
      instance.send(testRecord, testCallback)
      Thread.sleep(32)
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
      calledBack = true
    }
  }

}
