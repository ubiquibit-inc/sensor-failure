package com.ubiquibit.buoy.jobs

import com.ubiquibit.Wiring

/**
  * Bootstap the entire system, from "scratch".
  *
  * Pre-reqs:
  *
  * a. Data have been downloaded to the data directory
  * b. Redis is running at the location
  * c. Kafka is operational
  *
  * Note: (a,b,c) can be specified in application.propoerties (or as JVM args)
  *
  * P.S. Once bootstrapped, the system can drain off events from the Kafka queue,
  * but it is up to a future author to write teh queue appenders.
  */
object Bootstrap {

  def main(args: Array[String]) = {
    Wiring.initRedis.run()
    Wiring.initKafka.run()
  }

}
