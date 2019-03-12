package com.ubiquibit.buoy.jobs

import com.ubiquibit.Wiring

object Bootstrap {

  def main(args: Array[String]) = {
    Wiring.initRedis.run()
    Wiring.initKafka.run()
  }

}
