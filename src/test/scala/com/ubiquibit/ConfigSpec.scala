package com.ubiquibit

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.FunSpec
import org.scalatest.Matchers._

class ConfigSpec extends FunSpec{

  describe("Config"){

    it("should be loaded") {
      val config: Config = ConfigFactory.load()
      config.getString("data.directory") should be ("/tmp/test")
    }
  }

}
