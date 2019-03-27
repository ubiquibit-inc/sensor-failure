package com.ubiquibit.buoy.jobs.setup

import java.io.{BufferedWriter, File, FileWriter}

import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit.buoy._
import com.ubiquibit.{RandomElements, Wiring}

/**
  * Takes [[WxStation]] info out of [[com.redis.Redis]] and puts it into
  * into a staging file. When a downstream Spark stream
  * reads the contents, it kicks of an import from Kafka.
  *
  * @param env DI from [[Wiring]]
  */
class StageFeedsFromRedis(env: {
  val stationRepository: StationRepository
}) extends RandomElements {

  private val repo: StationRepository = env.stationRepository
  private val conf: Config = ConfigFactory.load()

  private val status: WxFeedStatus = KAFKALOADED

  def feeds(): Seq[(StationId, BuoyFeed)] = {

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

object StageFeedsFromRedis {

  def main(args: Array[String]): Unit = {
    Wiring.stageFromRedis.run(1000L)
  }

}