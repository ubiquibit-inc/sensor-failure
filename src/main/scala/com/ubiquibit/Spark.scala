package com.ubiquibit

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * An injectable trait - we can later switch to the idiomatic *.implicits._ usage...
  */
trait Spark {

  def session: SparkSession

  def makeSession(config: Seq[(String, String)]): SparkSession

  def sc: SparkContext

}

class SparkImpl extends Spark {

  val conf: Config = ConfigFactory.load()

  private val defaultConfigs: Seq[(String, String)] = Seq(
    ("spark.sql.shuffle.partitions", conf.getString("spark.partitions"))
  )

  private def buildIt(bldr: SparkSession.Builder, s: Seq[(String, String)]): SparkSession.Builder = {
    var bld = bldr
    for (cnf <- s) {
      bld = bldr.config(cnf._1, cnf._2)
    }
    bld
  }

  private val defaultBuilder: SparkSession.Builder = {
    val b = SparkSession.builder()
      .master(conf.getString("spark.master"))
    buildIt(b, defaultConfigs)
  }

  val session: SparkSession = {
    defaultBuilder.getOrCreate()
  }

  val sc: SparkContext = session.sparkContext

  override def makeSession(config: Seq[(String, String)]): SparkSession = {
    var b: SparkSession.Builder = defaultBuilder
    for( cnf <- config ){
      b = b.config(cnf._1, cnf._2)
    }
    b.getOrCreate()
  }

}
