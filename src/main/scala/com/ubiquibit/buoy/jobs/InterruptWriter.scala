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

import java.io.{BufferedWriter, File, FileWriter}
import java.util.logging.Logger

import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit.buoy.TextRecord
import org.apache.spark.sql.ForeachWriter

/**
  * This work (c) by jason@ubiquibit.com
  *
  * This work is licensed under a
  * Creative Commons Attribution-ShareAlike 4.0 International License.
  *
  * You should have received a copy of the license along with this
  *   work.  If not, see <http://creativecommons.org/licenses/by-sa/4.0/>.
  */
class InterruptWriter extends ForeachWriter[List[TextRecord]] {

  @transient private val Log: Logger = Logger.getLogger(getClass.getName)
  private val conf: Config = ConfigFactory.load()

  private var writer: BufferedWriter = null

  override def open(partitionId: Long, epochId: Long): Boolean = {
    val stageDir = conf.getString("interrupt.writer.out.dir")
    val tempFile = File.createTempFile(getClass.getName, ".out", new File(stageDir))
    writer = new BufferedWriter(new FileWriter(tempFile, true))
    true
  }

  override def close(errorOrNull: Throwable): Unit = {
    writer.flush()
    writer.close()
  }

  override def process(value: List[TextRecord]): Unit = {

    value.sliding(StationInterrupts.numRecords, StationInterrupts.numRecords).foreach { batch => {
      writer.newLine()
      batch.foreach(rec => {
        writer.write(s"$rec")
        writer.newLine()
      })
      writer.newLine()
      writer.flush()
    }

    }
  }

}
