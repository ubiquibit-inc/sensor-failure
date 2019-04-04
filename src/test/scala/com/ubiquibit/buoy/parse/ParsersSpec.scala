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

package com.ubiquibit.buoy.parse

import org.scalatest.FunSpec

class ParsersSpec extends FunSpec {

  private val spacey = "  z        b    c d  e  f "

  describe("singleSpace") {
    it("should preserve non-whitespace") {
      val output = Parsers.singleSpace(spacey)
      val expectedVals = Seq("z", "b", "c", "d", "e", "f")

      expectedVals.foreach(v => output.contains(v))

    }

    it("should single space") {
      val output = Parsers.singleSpace(spacey)

      assert(output.indexOf("  ") == -1)
    }
  }

}
