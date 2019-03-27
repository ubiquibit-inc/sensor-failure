package com.ubiquibit.buoy.jobs.setup

import com.ubiquibit.Wiring

/**
  * Bootstrap Redis
  *
  * Prequisites:
  *
  * a. Data files are in the data directory see: README.md for how to get it
  * b. (empty) Redis is available see: [[com.ubiquibit.Redis]] and /bash/boot-docker.sh
  *
  * Note: Settings for (a,b) can are specified in application.properties
  */
object Bootstrap {

  def main(args: Array[String]): Unit = {
    Wiring.initRedis.run()
    Wiring.initKafka.run()
  }

}
