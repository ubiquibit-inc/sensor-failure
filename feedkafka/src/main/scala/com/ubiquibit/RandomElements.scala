package com.ubiquibit

trait RandomElements {

  private val rand = scala.util.Random

  def randomElemOf[T](seq: Seq[T]): Option[T] = {
    if (seq.isEmpty) None
    else seq lift rand.nextInt(seq.length)
  }

}
