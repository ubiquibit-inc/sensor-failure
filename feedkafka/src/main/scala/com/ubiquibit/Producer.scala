package com.ubiquibit

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit.buoy._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

/**
  * A kafka producer, which will serve as a sink for WxRecords (during init/boot).
  *
  * @tparam WxRec message to send
  */
trait Producer[WxRec <: WxRecord] {

  /**
    * Sends to Kafka
    * @param value
    */
  def send(value: WxRec)

}

class KafkaProducerImpl(stationId: StationId,
                        buoyData: BuoyData,
                        callback: Callback) extends Producer[WxRecord] {

  private val topicName: String = {
    stationId.toString + "-" + buoyData.ext.toUpperCase
  }

  val conf: Config = ConfigFactory.load()

  private val bsServerProp = "bootstrap.servers"
  private val keySerializerProp = "key.serializer"
  private val valueSerializerProp = "value.serializer"

  private val kafkaProps: Properties = new Properties()
  kafkaProps.setProperty(bsServerProp, conf.getString(bsServerProp))
  kafkaProps.setProperty("retries", conf.getString("kafka.producer.retries"))
  kafkaProps.setProperty("linger.ms", conf.getString("kafka.linger.ms"))
  kafkaProps.setProperty("compression.type", conf.getString("kafka.compression.type"))
  kafkaProps.setProperty(keySerializerProp, conf.getString(keySerializerProp))

  kafkaProps.setProperty(valueSerializerProp, conf.getString(valueSerializerProp))

  val producer = new KafkaProducer[String, WxRecord](kafkaProps)

  override def send(value: WxRecord): Unit = {
    val record = new ProducerRecord[String, WxRecord](topicName, value)
    producer.send(record, callback)
  }
}

object Producers {

  def of(stationId: StationId, buoyData: BuoyData)(implicit stationRepository: StationRepository): Producer[WxRecord] = {
    val cb = new ProducerCallback(stationRepository, stationId, buoyData)
    // A callback per producer (instead of per message, which was how it was documented in the examples...?
    new KafkaProducerImpl(stationId, buoyData, cb)
  }

  class ProducerCallback(stationRepository: StationRepository, stationId: StationId, buoyData: BuoyData) extends Callback with Serializable {
    override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
      if (e != null) {
        println(s"Error sending message to Kafka: $recordMetadata")
        e.printStackTrace()
        stationRepository.updateImportStatus(stationId, buoyData, ERROR)
      }
    }
  }

}
