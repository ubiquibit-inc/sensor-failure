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

import com.redis._
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Basic redis support
  *
  * @see https://index.scala-lang.org/debasishg/scala-redis/redisclient/3.9?target=_2.12
  */
trait Redis {

  def client: RedisClient

}

class RedisImpl extends Redis{

  private val config: Config = ConfigFactory.load()
  private val host: String = config.getString("redis.host")
  private val port: Int = config.getInt("redis.port")

  override val client: RedisClient = new RedisClient(s"$host", s"$port".toInt)

}
