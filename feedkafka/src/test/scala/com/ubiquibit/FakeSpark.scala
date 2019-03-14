package com.ubiquibit
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

class FakeSpark extends Spark {
  override def spark: SparkSession = ???

  override def sc: SparkContext = ???
}


