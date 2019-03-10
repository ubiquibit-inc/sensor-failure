package com.ubiquibit

import com.redis._
import com.ubiquibit.buoy.StationId

/**
  * Basic redis support...
  *
  * @see https://index.scala-lang.org/debasishg/scala-redis/redisclient/3.9?target=_2.12
  */
trait Redis {

  val redis: RedisClient = new RedisClient("localhost", 6379)

}
