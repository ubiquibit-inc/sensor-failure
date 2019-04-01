package com.ubiquibit

import com.redis.RedisClient

class FakeRedis(rc: RedisClient) extends Redis {
  override def client: RedisClient = rc
}
