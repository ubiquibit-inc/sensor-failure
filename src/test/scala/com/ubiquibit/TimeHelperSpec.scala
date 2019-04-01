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

import java.time.ZonedDateTime
import java.util.{Calendar, GregorianCalendar, TimeZone}

import org.scalatest.FunSpec

class TimeHelperSpec extends FunSpec {

  describe("TimeHelper") {
    it("from") {
      val result = TimeHelper.from("2013-12-12 08:29:21")

      val cal = Calendar.getInstance()
      cal.setTime(result)

      assert(cal.get(Calendar.YEAR) === 2013 )
      assert(cal.get(Calendar.MONTH) + 1 === 12)
      assert(cal.get(Calendar.DATE) === 12)
      assert(cal.get(Calendar.HOUR) === 8)
      assert(cal.get(Calendar.MINUTE) === 29)
      assert(cal.get(Calendar.SECOND) === 21)
      assert(cal.get(Calendar.MILLISECOND) === 0)

    }
  }

}
