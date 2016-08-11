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

  override def _advance: Option[(Locus, Coverage)] = {
    var lowerLimit = math.max(0, locus - halfWindowSize)
    var upperLimit = locus + halfWindowSize

    while (ends.headOption.exists(_ <= lowerLimit)) {
      ends.dequeue()
    }

    if (ends.isEmpty) {
      if (intervals.isEmpty)
        return None

      val nextReadWindowStart = intervals.head.start - halfWindowSize
      if (nextReadWindowStart > locus) {
        skipTo(nextReadWindowStart)
        upperLimit = locus + halfWindowSize
      }
    }

    var numAdded = 0
    while (intervals.nonEmpty && intervals.head.start <= upperLimit) {
      val Interval(start, end) = intervals.next()

      if (start < lastStart)
        throw RegionsNotSortedException(
          s"Consecutive regions rewinding from $lastStart to $start on contig ${intervals.contigName}"
        )
      else
        lastStart = start

      ends.enqueue(end)
      numAdded += 1
    }

    Some(locus -> Coverage(ends.size, numAdded))
  }
}

case class RegionsNotSortedException(msg: String) extends Exception
