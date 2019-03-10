package com.ubiquibit.buoy.jobs

import java.io.File

import com.ubiquibit.{KafkaTopics, StationRepository}
import com.ubiquibit.buoy.{BuoyData, Ocean, Text}
import org.apache.spark.sql.DataFrame

object InitKafka extends KafkaTopics with StationRepository{

  import com.ubiquibit.buoy.{FileReckoning => FR}

  val supportedTypes: Set[BuoyData] = Set(Text)
  val supported: (String) => Boolean = { absolutePath => supportedTypes.exists(_.same(new File(absolutePath))) }

  def supportedFiles: List[File] = {
    FR.filenames(fq=true)
      .filter(supported)
      .map(new File(_))
  }

  def readFile(): DataFrame = {
    ???
  }

  def main(args: Array[String]): Unit = {
    val f = supportedFiles.head

    val x = 23
  }

  // - See what's in the realtime 2 directory
  //   - Write a meta-data record (to Redis) for every Buoy station ID
  //   b. calculate phase for every combination of station id, data format relevant to this station (upsert to Redis)
  //   c. convert to canonical (case class) model
  //   c. write data to Kafka (using canonical model)

}
