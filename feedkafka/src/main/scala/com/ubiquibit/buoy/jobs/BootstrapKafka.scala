package com.ubiquibit.buoy.jobs

import com.ubiquibit.Wiring

/**
  *
  * Parses supported data feeds from files in the data directory and
  * puts the records into Kafka
  *
  * Pre-reqs:
  *
  * a. Redis has been bootstrapped (see: [[BootstrapRedis]])
  * b. Data files are ready in data directory (see: application.properties)
  * c. Kafka is up and running (see application.properties)
  * d. topics have been created for each supported file type (see /bash/create-kafka-topics.sh)
  *
  */
object BootstrapKafka {

  def main(args: Array[String]): Unit = {
    Wiring.initKafka.run()
  }

}
