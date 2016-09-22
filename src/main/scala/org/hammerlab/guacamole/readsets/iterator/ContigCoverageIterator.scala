package org.hammerlab.guacamole.readsets.iterator

import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.iterator.{SkippableLociIterator, SkippableLocusKeyedIterator}
import org.hammerlab.guacamole.reference.{ContigIterator, Interval, Locus}

import scala.collection.mutable

/**
 * [[SkippableLociIterator]] that consumes reference regions (sorted by start-position, and all on the same contig) and
 * emits a [[Coverage]] at each covered locus, indicating how many reads overlap (and start at) that locus. Overlaps
 * are given a grace-margin of `halfWindowSize`.
 *
 * @param intervals Single-contig-restricted, start-position-sorted [[Iterator]] of [[Interval]]s.
 */
case class ContigCoverageIterator(halfWindowSize: Int,
                                  intervals: ContigIterator[Interval])
  extends SkippableLocusKeyedIterator[Coverage] {

  // Queue of intervals that overlap the current locus, ordered by ascending `end`.
  private val ends = mutable.PriorityQueue[Long]()(implicitly[Ordering[Long]].reverse)

  // Record the last-seen interval-start, to verify that the intervals are sorted by `start`.
  private var lastStart: Locus = 0

  /**
   * If `skipTo` is called on this iterator, leaving it mid-way through some reads whose "starts" were neve
   * counted/emitted, we record that here, so that the total number of "read starts" emitted matches the total number of
   * reads that contribute any depth to a (potentially scattered/sparse) set of loci that are evaluated / skipped
   * across by callers.
   */
  private var pendingStarts: Int = 0

  override def _advance: Option[(Locus, Coverage)] = {
    val endLowerBound = math.max(0, locus - halfWindowSize)

    while (ends.headOption.exists(_ <= endLowerBound)) {
      ends.dequeue()
    }

    val startUpperBound = locus + halfWindowSize

    var numAdded = pendingStarts

    while (intervals.nonEmpty && intervals.head.start <= startUpperBound) {
      val Interval(start, end) = intervals.next()

      if (start < lastStart)
        throw RegionsNotSortedException(
          s"Consecutive regions rewinding from $lastStart to $start on contig ${intervals.contigName}"
        )
      else
        lastStart = start

      if (end + halfWindowSize > locus) {
        ends.enqueue(end)
        numAdded += 1
      }
    }

    pendingStarts = numAdded

    if (ends.isEmpty)
      if (intervals.isEmpty)
        None
      else {
        skipTo(intervals.head.start - halfWindowSize)
        _advance
      }
    else
      Some(locus -> Coverage(ends.size, numAdded))
  }

  override def postNext(): Unit = {
    super.postNext()
    pendingStarts = 0
  }
}

case class RegionsNotSortedException(msg: String) extends Exception(msg)
