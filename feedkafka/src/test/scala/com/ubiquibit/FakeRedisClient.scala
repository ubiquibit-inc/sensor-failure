package com.ubiquibit

import com.redis.RedisClient
import com.redis.serialization.{Format, Parse}
import com.ubiquibit.buoy.StationInfo

class FakeRedisClient extends RedisClient {

  var readStationResponse: Seq[StationInfo] = Seq()

  var hmgetResult: Map[Any, String] = Map.empty
  var setCount = 0

  override def hmset(key: Any, map: Iterable[Product2[Any, Any]])(implicit format: Format): Boolean = {
    setCount = setCount + 1
    true
  }

  var getCount = 0

  override def hmget[K, V](key: Any, fields: K*)(implicit format: Format, parseV: Parse[V]): Option[Map[K, V]] = {
    getCount = getCount + 1
    if (hmgetResult.isEmpty) None
    else Some(hmgetResult).asInstanceOf[Some[Map[K, V]]]
  }

  var delCount = 0

  override def del(key: Any, keys: Any*)(implicit format: Format): Option[Long] = {
    delCount = delCount + 1
    None
  }

}
