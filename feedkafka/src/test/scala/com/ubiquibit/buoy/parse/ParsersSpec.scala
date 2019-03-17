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
