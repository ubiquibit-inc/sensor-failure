package com.ubiquibit

import com.redis._
import com.ubiquibit.buoy.StationId

/**
  * Basic redis support...
  *
  * @see https://index.scala-lang.org/debasishg/scala-redis/redisclient/3.9?target=_2.12
  */
object Redis {

  val redis: RedisClient = new RedisClient("localhost", 6379)
  val freqField = "observationFrequencyMins"

  import com.ubiquibit.buoy.FileReckoning._

  def saveStations(): Unit = {
    for (rk <- stationIds.map(redisKey)) {
      redis.hmset(rk, Map(freqField -> "0"))
    }
  }

  def readStations(): Unit = {
    for (rk <- stationIds.map(redisKey)) {
      val freqVal = redis.hmget(rk, freqField)
      println(s"$rk [$freqVal]")
    }
  }

  protected def redisKey(stationId: StationId): String = {
    require(stationId.toString.length > 3)
    s"stationId:$stationId"
  }

  //  def main(args: Array[String]) = {
  //    saveStations()
  //    readStations()
  //  }

}
