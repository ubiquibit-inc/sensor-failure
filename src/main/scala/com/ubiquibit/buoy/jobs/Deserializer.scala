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
