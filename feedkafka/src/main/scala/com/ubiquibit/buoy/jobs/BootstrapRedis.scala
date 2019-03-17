package com.ubiquibit.buoy.jobs

import com.ubiquibit.Wiring

/**
  * Bootstrap Redis
  *
  * a. Data files are in the data directory
  * b. (empty) Redis is available
  *
  * Note: Settings for (a,b) can are specified in application.properties
  */
object BootstrapRedis {

  def main(args: Array[String]) = {
    Wiring.initRedis.run()
  }

}
