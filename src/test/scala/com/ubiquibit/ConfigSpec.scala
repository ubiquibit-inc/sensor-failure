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
