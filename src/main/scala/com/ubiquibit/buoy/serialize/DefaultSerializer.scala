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

package com.ubiquibit.buoy.serialize

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util

import com.ubiquibit.buoy.WxRecord
import org.apache.kafka.common.serialization.Serializer

/**
  * @see https://stackoverflow.com/questions/18160045/fastest-serialization-deserialization-of-scala-case-classes/18161724
  */
class DefaultSerializer extends Serializer[WxRecord] {

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {}

  override def serialize(s: String, rec: WxRecord): Array[Byte] = {
    val byteOut = new ByteArrayOutputStream()
    val objOut = new ObjectOutputStream(byteOut)
    objOut.writeObject(rec)
    objOut.close()
    byteOut.close()
    byteOut.toByteArray
  }

  override def close(): Unit = {}

}
