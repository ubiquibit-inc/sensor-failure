/*
 * Copyright (c) 2019.
 *
 * This work, (c) by jason@ubiquibit.com
 *
 * This work is licensed under a
 * Creative Commons Attribution-ShareAlike 4.0 International License.
 *
 * You should have received a copy of the license along with this
 * work.  If not, see <http://creativecommons.org/licenses/by-sa/4.0/>.
 *
 */

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
    *
    * @param value
    */
  def send(value: WxRec, callback: Callback)

}

trait TopicNamer {
  def topicName(stationId: StationId, buoyData: WxFeed): String = {
    stationId.toString + "-" + buoyData.ext.toUpperCase
  }
  def topicName(stationId: StationId, wxFeed: String): String = {
    stationId.toString + "-" + wxFeed.toUpperCase()
  }
}

class KafkaProducerImpl(stationId: StationId,
                        buoyData: WxFeed,
                        callback: Callback) extends Producer[WxRecord] with TopicNamer {

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

  override def send(value: WxRecord, callback: Callback = callback): Unit = {
    val record = new ProducerRecord[String, WxRecord](topicName(stationId, buoyData), value)
    producer.send(record, callback)
  }
}

object Producers {

  def of(stationId: StationId, buoyData: WxFeed, cb: Option[Callback] = None)(implicit stationRepository: StationRepository): Producer[WxRecord] = {
    if (cb.isEmpty) new KafkaProducerImpl(stationId, buoyData, new ProducerCallback(stationRepository, stationId, buoyData))
    else new KafkaProducerImpl(stationId, buoyData, cb.get)
  }

  class ProducerCallback(stationRepository: StationRepository, stationId: StationId, buoyData: WxFeed) extends Callback with Serializable {
    override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
      if (e != null) {
        println(s"Error sending message to Kafka: $recordMetadata")
        e.printStackTrace()
        stationRepository.updateImportStatus(stationId, buoyData, ERROR)
      }
    }
  }

}
