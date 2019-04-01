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
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{FunSpec, Tag}

abstract class SparkSpec extends FunSpec{

  private val conf: Config = ConfigFactory.load()
  private val master = conf.getString("spark.master")
  private val appName = getClass.getName

  private val sparkConf: SparkConf = new SparkConf()
    .setAppName(appName)
    .setMaster(master)
    .set("spark.driver.allowMultipleContexts", "false")
    .set("spark.ui.enabled", "false")

  val ss: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  val sc: SparkContext = ss.sparkContext
  val sqlContext: SQLContext = ss.sqlContext

}

object SparkTest extends Tag("SparkTest")
