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
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

class FakeSpark extends Spark {
  override def session: SparkSession = {
    null
  }

  override def sc: SparkContext = ???

  override def makeSession(config: Seq[(String, String)]): SparkSession = {
    null
  }
}


