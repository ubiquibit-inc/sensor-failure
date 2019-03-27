package com.ubiquibit.buoy

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit.{FakeFile, Wiring}
import org.scalatest.FunSpec
import StationId.makeStationId

class FileReckoningSpec extends FunSpec {

  private val fullMonty = Wiring.fileReckoning

  val (fmt0, fmt1) = (Ocean, Swr2)

  val instance: FileReckoning with SupportedFeeds = new FileReckoning with SupportedFeeds {

    override def stationIds: Seq[StationId] = Seq("abcde", "123123").map(makeStationId)

    override def feeds: Map[StationId, Seq[BuoyData]] = Map(stationIds.head -> Seq(fmt1, fmt0), stationIds()(1) -> Seq(fmt1))

    override def getFile(stationId: StationId, ofType: BuoyData): Option[File] = Some(new FakeFile(stationId.toString))

    override def stationInfo(): Seq[WxStation] = ???

    override def pairs(): List[(StationId, BuoyData)] = ???
  }

  val config: Config = ConfigFactory.load()

  private def createIfNecessary(dir: File, fn: String): Option[File] = {
    assert(dir.isDirectory && dir.canWrite)
    val newFile = new File(dir, fn)
    if (newFile.exists() || newFile.createNewFile()) Some(newFile)
    else None
  }

  private def fixture =
    new {
      private val dir = config.getString("data.directory")
      private val subdir = config.getString("buoy.data.subdir")
      private val f = new File(s"$dir$subdir")
      assert(f.exists || f.mkdirs())
      assert(f.exists && f.canWrite)
      private val created = instance.supported.map { t =>
        createIfNecessary(f, "abcdef." + t.ext)
        createIfNecessary(f, "bcdefg." + t.ext)
        createIfNecessary(f, "cdefgh." + t.ext)
      }
      assert(created.size === created.flatten.size) // make sure createNewFile never failed
    }

  describe("FileReckoning should") {
    it("return a stationId (where data file is in a supported format for said station)") {

      fixture

      instance.stationIds.map(_.toString) contains "a"
      instance.stationIds.map(_.toString) contains "b"
      instance.stationIds.map(_.toString) contains "c"

    }

    it("return a File for StationId and supported BuoyData") {

      fixture

      val stationId = instance.stationIds.head
      val typeOf = instance.supported.head

      val result = instance.getFile(stationId, typeOf)

      assert(result.isDefined)

      val file = result.get
      assert(file.exists() && file.isFile)

    }

    it("return BuoyData for each station with a supported data feed") {

      fixture

      val result = instance.feeds

      assert(result.count(_._2.contains(Swr2)) === 2)
      assert(result.count(_._2.contains(Ocean)) === 1)
      assert(result.size === 2)

    }

  }

}
