package com.ubiquibit

import com.redis._
import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit.buoy.StationId

/**
  * Basic redis support...
  *
  * @see https://index.scala-lang.org/debasishg/scala-redis/redisclient/3.9?target=_2.12
  */
trait Redis {

  private val config: Config = ConfigFactory.load()
  private val host: String = config.getString("redis.host")
  private val port: Int = config.getInt("redis.port")

  val redis: RedisClient = new RedisClient(s"$host", s"$port".toInt)

}
