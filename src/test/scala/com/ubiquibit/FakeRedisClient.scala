package com.ubiquibit

import com.redis.RedisClient
import com.redis.serialization.{Format, Parse}
import com.ubiquibit.buoy.WxStation

class FakeRedisClient extends RedisClient {

  def reset() = {
    setCount = 0
    getCount = 0
    delCount = 0
    keysCount = 0
    fakeHmgetResult = Map()
    fakeKeys = None
  }

  var setCount = 0
  override def hmset(key: Any, map: Iterable[Product2[Any, Any]])(implicit format: Format): Boolean = {
    setCount = setCount + 1
    true
  }

  var getCount = 0

  var fakeHmgetResult: Map[String, String] = Map()

  override def hmget[K, V](key: Any, fields: K*)(implicit format: Format, parseV: Parse[V]): Option[Map[K, V]] = {
    getCount = getCount + 1
    if (fakeHmgetResult.isEmpty) None
    else Some(fakeHmgetResult.asInstanceOf[Map[K,V]])
  }

  var delCount = 0

  override def del(key: Any, keys: Any*)(implicit format: Format): Option[Long] = {
    delCount = delCount + 1
    None
  }

  var keysCount = 0

  var fakeKeys: Option[List[Option[String]]] = None
  override def keys[A](pattern: Any)(implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] = {
    keysCount = keysCount + 1
    fakeKeys.asInstanceOf[Option[List[Option[A]]]]
  }

}
