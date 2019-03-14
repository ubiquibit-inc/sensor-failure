package com.ubiquibit.buoy.serialize

import com.ubiquibit.buoy.{BuoyData, WxRecord}
import org.apache.kafka.common.serialization.Serializer

object Serializers {


  def of(buoyData: BuoyData): Serializer[WxRecord] = new DefaultSerializer

}

object DefSer {

  import java.io._

  def serialize[T <: Serializable](obj: T): Array[Byte] = {
    val byteOut = new ByteArrayOutputStream()
    val objOut = new ObjectOutputStream(byteOut)
    objOut.writeObject(obj)
    objOut.close()
    byteOut.close()
    byteOut.toByteArray
  }

  def deserialize[T <: Serializable](bytes: Array[Byte]): T = {
    val byteIn = new ByteArrayInputStream(bytes)
    val objIn = new ObjectInputStream(byteIn)
    val obj = objIn.readObject().asInstanceOf[T]
    byteIn.close()
    objIn.close()
    obj
  }
}