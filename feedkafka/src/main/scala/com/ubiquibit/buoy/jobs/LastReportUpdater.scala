package com.ubiquibit.buoy.jobs

import java.sql.Timestamp

import com.ubiquibit.buoy.{StationId, StationRepository}
import org.apache.spark.sql.{ForeachWriter, Row}

class LastReportUpdater(stationId: StationId) extends ForeachWriter[Row] with Serializable {

  override def open(partitionId: Long, epochId: Long): Boolean = {true}

  override def process(value: Row): Unit = {
    val t = value.get(0)
//    val ts: Timestamp = new java.sql.Timestamp(t)
    println(s"Found a new max timestamp: $t")
//    repo.updateLastReport(stationId, ts)
  }

  override def close(errorOrNull: Throwable): Unit = {}
}
