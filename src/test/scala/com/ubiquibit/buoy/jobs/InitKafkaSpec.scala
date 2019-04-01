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

package com.ubiquibit.buoy.jobs

import com.ubiquibit.buoy.jobs.setup.InitKafkaImpl
import com.ubiquibit.{FakeSpark, FakeStationRepository, Spark}
import com.ubiquibit.buoy.{FakeFileReckoning, StationRepository}
import org.scalatest.{BeforeAndAfter, FunSpec}

class InitKafkaSpec extends FunSpec with BeforeAndAfter {

  val stationRepository: StationRepository = new FakeStationRepository
  private val fakeRepo = stationRepository.asInstanceOf[FakeStationRepository]
  val spark: Spark = new FakeSpark
  val fileReckoning = new FakeFileReckoning

  val instance = new InitKafkaImpl(env = this)

  after {
    fakeRepo.reset
  }

  ignore("InitKafka should") {

    it("ask repo for StationInfo") {

      instance.run(None)

      assert(fakeRepo.readStationsCount == 1)

    }

  }

}