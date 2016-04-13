package org.hammerlab.guacamole.loci

import java.lang.{Long => JLong}

import com.google.common.collect.{Range => JRange}

import scala.collection.Iterator

/**
 * A range of Longs. Inclusive on start, exclusive on end.
 */
case class SimpleRange(start: Long, end: Long) extends Ordered[SimpleRange] {
  /** Iterate through elements in the range. */
  def iterator(): Iterator[Long] = new Iterator[Long] {
    private var i = start
    override def hasNext: Boolean = i < end
    override def next(): Long =
      if (hasNext) { val result = i; i += 1; result }
      else Iterator.empty.next()
  }
  /** Number of elements in the range. */
  def length: Long = end - start

  /** Comparisons between ranges. Order is DESCENDING (i.e. reversed) from by start. */
  def compare(other: SimpleRange): Int = {
    val diff = start - other.start
    if (diff < 0) -1
    else if (diff == 0) 0
    else 1
  }
}

object SimpleRange {
  def apply(range: JRange[JLong]): SimpleRange = SimpleRange(range.lowerEndpoint(), range.upperEndpoint())
}
