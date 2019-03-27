package com.ubiquibit.buoy.jobs

import java.io.{BufferedWriter, File, FileWriter}

import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit.{RandomElements, Wiring}
import com.ubiquibit.buoy._

class StageFromRedis(env: {
  val stationRepository: StationRepository
}) extends RandomElements {

  private val repo: StationRepository = env.stationRepository
  private val conf: Config = ConfigFactory.load()

  private val status: ImportStatus = DONE

  def feeds(): Seq[(StationId, BuoyData)] = {

    val feeds = for (wxStation <- repo.readStations()
                         if wxStation.feeds.exists(f => f._2 == status))
      yield wxStation
        .feeds
        .filter(f => f._2 == status)
        .keys
        .map((wxStation.stationId, _))

    feeds.flatten
  }

  def run(snoozeMs: Long): Unit = {

    val stageFile = conf.getString("stage.file")
    val writer = new BufferedWriter(new FileWriter(new File(stageFile), true))

    for( f <- feeds() ) {

      writer.write(s"${f._1},${f._2}")
      writer.newLine()
      writer.flush()

      Thread.sleep(snoozeMs)

    }

    writer.flush()
    writer.close()

  }

}

object StageFromRedis {

  def main(args: Array[String]): Unit = {
    Wiring.stageFromRedis.run(1000L)
  }

}