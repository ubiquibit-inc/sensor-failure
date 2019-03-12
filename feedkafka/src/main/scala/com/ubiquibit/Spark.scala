package com.ubiquibit

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait Spark {

  val spark: SparkSession = SparkSession.builder()
    .appName("InitKafkaSpec")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "5")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

}
