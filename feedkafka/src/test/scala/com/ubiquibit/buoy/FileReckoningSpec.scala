package com.ubiquibit.buoy

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import com.ubiquibit.Wiring
import org.scalatest.FunSpec

class FileReckoningSpec extends FunSpec{

  val instance: FileReckoning = Wiring.fileReckoning

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
      private val subdir = config.getString("bouy.data.subdir")
      private val f = new File(s"$dir$subdir")
      assert(f.exists || f.mkdirs())
      assert(f.exists && f.canWrite)
      private val created = instance.supportedTypes.map { t =>
        createIfNecessary(f, "abcdef." + t.ext)
        createIfNecessary(f, "bcdefg." + t.ext)
        createIfNecessary(f, "cdefgh." + t.ext)
      }
      assert(created.size === created.flatten.size) // make sure createNewFile never failed
    }

  describe("FileReckoning: ") {
    it("returns a stationId per fixture file") {

      fixture

      instance.stationIds.map(_.toString) contains "a"
      instance.stationIds.map(_.toString) contains "b"
      instance.stationIds.map(_.toString) contains "c"

    }

    it("returns a File for StationId and supported BuoyData"){

      fixture

      val stationId = instance.stationIds.head
      val typeOf  = instance.supportedTypes.head

      val result = instance.getFile(stationId, typeOf)

      assert( result.isDefined )

      val file = result.get
      assert( file.exists() && file.isFile )

    }

    it("returns a BuoyData for each station with a supported data feed"){

      fixture

      val result = instance.supportByStation

      // this test is stinky because it works by coincidence (the fixture made it do it). fixture
      // should be a little random and interrogable...

      assert( result.count(_._2.contains(Text)) === 3 )
      assert( result.size === 3)

    }

  }

}
