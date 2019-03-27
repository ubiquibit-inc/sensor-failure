package com.ubiquibit.buoy.jobs

import com.ubiquibit.buoy.TextRecord
import com.ubiquibit.buoy.serialize.DefSer
import org.apache.spark.sql.Row

trait Deserializer {
  // TODO pull out decoder/deserializer logic to allow loading by BuoyData feed type
  def deserialize(row: Row): TextRecord = {
    DefSer.deserialize(row.getAs[Array[Byte]]("value")).asInstanceOf[TextRecord]
  }
}
