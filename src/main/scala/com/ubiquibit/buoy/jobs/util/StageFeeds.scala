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

package com.ubiquibit.buoy.jobs.util

import java.io.{BufferedWriter, File, FileWriter}

import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit.buoy._
import com.ubiquibit.{RandomElements, Wiring}

/**
  * Takes [[WxStation]] info out of [[com.redis.Redis]] and puts it into
  * into a staging temp file in the staging directory. When a downstream Spark stream
  * reads the contents, it kicks of an import from Kafka.
  *
  * @param env DI from [[Wiring]]
  */
class StageFeeds(env: {
  val stationRepository: StationRepository
}) extends RandomElements {

  private val repo: StationRepository = env.stationRepository
  private val conf: Config = ConfigFactory.load()

  private val status: WxFeedStatus = KAFKALOADED

  def feeds(): Seq[(StationId, WxFeed)] = {

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

    val stageDir = conf.getString("stage.dir")
    val tempFile = File.createTempFile("FeedStage", ".csv", new File(stageDir))
    val writer = new BufferedWriter(new FileWriter(tempFile, true))

    for (f <- feeds()) {

      writer.write(s"${f._1},${f._2}")
      writer.newLine()
      writer.flush()

      Thread.sleep(snoozeMs)

      repo.updateImportStatus(f._1, f._2, SPARKSTAGED)

    }

    writer.flush()
    writer.close()

  }

}

object StageFeeds {

  def main(args: Array[String]): Unit = {
    Wiring.stageFromRedis.run(1000L)
  }

}