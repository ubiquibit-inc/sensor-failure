package com.ubiquibit

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait Spark {

  def spark: SparkSession

  def sc: SparkContext

}

class SparkImpl extends Spark {

  val conf: Config = ConfigFactory.load()

  val spark: SparkSession = SparkSession.builder()
    .appName("InitKafkaSpec")
    .master(conf.getString("spark.master"))
    .config("spark.sql.shuffle.partitions", conf.getString("spark.partitions"))
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

}
