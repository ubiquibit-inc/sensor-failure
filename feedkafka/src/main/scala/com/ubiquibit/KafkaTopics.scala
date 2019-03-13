package com.ubiquibit

/**
  * A way to create topics programatically (w/o the help of scripts)
  */
trait KafkaTopics {

  def createIfNeeded(topic: String): Option[String] = ???

}
