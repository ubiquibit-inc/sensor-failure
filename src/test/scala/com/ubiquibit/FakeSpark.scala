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


