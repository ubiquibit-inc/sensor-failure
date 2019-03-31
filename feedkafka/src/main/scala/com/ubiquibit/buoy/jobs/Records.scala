package com.ubiquibit.buoy.jobs

import java.sql.Timestamp

import com.ubiquibit.buoy.TextRecord

import scala.collection.mutable

/**
  * A minimal heap implementation, so that we can maintain a fixed number of records per Station [[WxStream]]
  *
  * @param maxSize capacity - records older than this will be dumped
  */
class Records(maxSize: Int) extends Serializable {

  implicit def timestampOrdering: Ordering[Timestamp] = Ordering.fromLessThan(_ after _)

  val q: mutable.PriorityQueue[TextRecord] = mutable.PriorityQueue.empty[TextRecord] {
    Ordering.by(_.eventTime)
  }

  private val deduplicate = true

  def push(rec: TextRecord): Records = {
    if (deduplicate && all().contains(rec)) return this
    q += rec
    if (q.size > maxSize) q.dequeue()
    this
  }

  def all(): Seq[TextRecord] = {
    q.clone().dequeueAll.iterator.toList
  }

  def iterator(): Iterator[TextRecord] = q.iterator

  def size: Int = q.size

  /**
    * @return 0 to 2 most recent records, newest at position 0
    */
  def recents(): Seq[TextRecord] = {
    if (q.size > 1) {
      Seq(q.last, q.toList(q.size - 2))
    }
    else if (q.size == 1) {
      Seq(q.last)
    }
    else {
      Seq()
    }
  }

}
