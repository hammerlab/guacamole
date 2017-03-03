package org.hammerlab.guacamole.readsets.iterator

import org.hammerlab.genomics.loci.iterator.{ SkippableLociIterator, SkippableLocusKeyedIterator }
import org.hammerlab.genomics.reference.{ ContigIterator, Interval, Locus }
import org.hammerlab.guacamole.loci.Coverage

import scala.collection.mutable

/**
 * [[SkippableLociIterator]] that consumes [[Interval]]s (sorted by start-position) and emits a [[Coverage]] at each
 * covered locus, indicating how many intervals overlap (and start at) that locus. Overlaps are given a grace-margin of
 * `halfWindowSize`.
 *
 * One ([[Locus]], [[Coverage]]) tuple is emitted for each locus where the underlying intervals (and `halfWindowSize`)
 * yield a non-empty [[Coverage]]: intervals contribute 1 to [[Coverage.depth]] for each locus they overlap
 * (± halfWindowSize) and 1 to [[Coverage.starts]] for the first such locus.
 *
 * In cases where this iterator's [[SkippableLociIterator.skipTo]] method is called while it is emitting elements (e.g.
 * when this iterator is made to proceed through loci discontinuously, to restrict calculation to specific loci-ranges),
 * [[ContigCoverageIterator]] can land in the middle of some of its input [[Interval]]s, emitting their first
 * contributions to [[Coverage.depth]] at loci that are not the start of those [[Interval]]s. In these (and all) cases,
 * [[Interval]]s contribute to [[Coverage.starts]] on the first locus where they contribute to
 * [[Coverage.depth]] (even if this is in the middle of the [[Interval]] due to upstream loci-restrictions and
 * skipping).
 *
 * @param intervals Single-contig-restricted, start-position-sorted [[Iterator]] of [[Interval]]s.
 */
case class ContigCoverageIterator(halfWindowSize: Int,
                                  intervals: ContigIterator[Interval])
  extends SkippableLocusKeyedIterator[Coverage] {

  /**
   * We maintain two queues of "active" intervals (actually, just their [[Interval.end]] values) that have been pulled
   * from [[intervals]] and overlap the last-evaluated [[locus]].
   *
   * Both process the [[Interval]]s from [[intervals]], which are ordered by ascending [[Interval.start]], and page them
   * out in order of ascending [[Interval.end]].
   *
   * [[startEnds]] is cleared by [[postNext]] every time [[next]] is called, as we ensure that each [[Interval]] is
   * counted towards [[Coverage.starts]] on the first locus where it contributes to [[Coverage.depth]] (that this
   * iterator is evaluated at, as opposed to skipped over), and only that locus.
   */
  private val startEnds = mutable.PriorityQueue[Locus]()(implicitly[Ordering[Locus]].reverse)
  private val depthEnds = mutable.PriorityQueue[Locus]()(implicitly[Ordering[Locus]].reverse)

  /**
   * Record the last-seen interval-start, to verify that the intervals are sorted by `start`.
   */
  private var lastStart = Locus(0)

  override def _advance: Option[(Locus, Coverage)] = {
    // Intervals whose ends are below (or equal to) this bound will be paged out of the queues.
    val endLowerBound = locus - halfWindowSize

    while (depthEnds.headOption.exists(_ <= endLowerBound)) {
      depthEnds.dequeue()
    }

    while (startEnds.headOption.exists(_ <= endLowerBound)) {
      startEnds.dequeue()
    }

    // Intervals whose start is less than (or equal to) this bound will be counted / added to the queues.
    val startUpperBound = locus + halfWindowSize

    /**
     * Process all intervals that have come into range of [[locus]] since the last [[_advance]] call.
     *
     * If [[SkippableLociIterator.skipTo]] has been called, we may discard whole [[Interval]]s that were skipped over.
     */
    while (intervals.nonEmpty && intervals.head.start <= startUpperBound) {
      val Interval(start, end) = intervals.next()

      // Check and update state related to verifying that intervals are ordered by ascending [[Interval.start]].
      if (start < lastStart)
        throw RegionsNotSortedException(
          s"Consecutive regions rewinding from $lastStart to $start on contig ${intervals.contigName}"
        )
      else
        lastStart = start

      /**
       * Verify that the interval we're processing overlaps the current locus; if [[locus]] has been skipped ahead since
       * the last time [[_advance]] was called, we may have to implicitly drop some intervals here (that were completely
       * skipped over) before we arrive at ones that actually overlap our current [[locus]].
       */
      if (end > endLowerBound) {
        depthEnds.enqueue(end)
        startEnds.enqueue(end)
      }
    }

    /**
     * If we are out of intervals, or we've been skipped to a location with no coverage, we land here with no coverage
     * to return:
     *
     *   - in the former case, this iterator is done, and we return [[None]].
     *   - in the latter case, skip to the start of the next interval (the first locus where this iterator can emit a
     *     valid tuple) and recurse.
     */
    if (depthEnds.isEmpty)
      if (intervals.isEmpty)
        None
      else {
        skipTo(intervals.head.start - halfWindowSize)
        _advance
      }
    else
      /**
       * Return a valid ([[Locus]], [[Coverage]]) tuple, indicating the number of intervals covering (and starting at /
       * covering for the first time) the current [[locus]].
       */
      Some(locus -> Coverage(depthEnds.size, startEnds.size))
  }

  override def postNext(): Unit = {
    super.postNext()
    startEnds.clear()
  }
}

case class RegionsNotSortedException(msg: String) extends Exception(msg)
