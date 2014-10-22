package org.bdgenomics.guacamole.windowing

import org.bdgenomics.guacamole.{HasReferenceRegion, LociMap}

/**
 *
 * This iterator moves locus by locus through a collection of sliding windows.
 *
 * Each call to next advances each SlidingWindow one locus (unless skipEmpty is set where it advances to the next
 * non-empty locus).
 *
 * @param ranges Collection of ranges the iterator should cover
 * @param skipEmpty Skip loci if the windows contain no overlapping regions
 * @param restWindows Sliding Window of regions
 */
case class SlidingWindowsIterator[Region <: HasReferenceRegion](ranges: Iterator[LociMap.SimpleRange],
                                                                skipEmpty: Boolean,
                                                                headWindow: SlidingWindow[Region],
                                                                restWindows: Seq[SlidingWindow[Region]])
  extends Iterator[Seq[SlidingWindow[Region]]] {

  private val windows: Seq[SlidingWindow[Region]] = headWindow :: restWindows.toList

  private var currentRange: LociMap.SimpleRange = ranges.next()

  private var currentLocus: Long = -1L
  private var nextLocus: Option[Long] =
    if (skipEmpty)
      findNextNonEmptyLocus(currentRange.start)
    else
      Some(currentRange.start)

  // Check that there are regions in the window and the final base of the final region is before this locus
  private def currentElementsOverlapLocus(locus: Long): Boolean =
    windows.exists(_.endOfRange().exists(locus < _))

  // Check that the next element in any window overlaps [[locus]]
  private def nextElementOverlapsLocus(locus: Long): Boolean =
    windows.exists(_.nextElement.exists(_.overlapsLocus(locus)))

  /**
   * Whether there are any more loci left to process
   *
   * @return true if there are locus left to process and if skipEmpty is true, there is a locus with overlapping
   *         elements
   */
  override def hasNext: Boolean = {
    nextLocus.isDefined
  }

  /**
   * Next returns the sequences of windows we are iterating over, which have been advanced to the next locus
   * The window cover a region of (2 * halfWindowSize + 1) around the currentLocus
   * and contain all of the elements that overlap that region
   *
   * Each call to next() returns windows that been advanced to the next locus (and corresponding overlapping reads are
   * adjusted)
   *
   * @return The sliding windows advanced to the next locus (next nonEmpty locus if skipEmpty is true)
   */
  override def next(): Seq[SlidingWindow[Region]] = {
    nextLocus match {
      case Some(locus) => {
        currentLocus = locus
        windows.foreach(_.setCurrentLocus(currentLocus))
        nextLocus = findNextLocus(currentLocus)
        windows
      }
      case None => throw new ArrayIndexOutOfBoundsException()
    }
  }

  /**
   * Identifies the next locus to process after [[locus]]
   *
   * @param locus Current locus that iterator is at
   * @return Next locus > [[locus]] to process, None if none exist
   */
  def findNextLocus(locus: Long): Option[Long] = {
    var nextLocusInRangeOpt = nextLocusInRange(locus + 1)

    // If we are skipping empty loci and the the current window does not overlap the next locus
    if (skipEmpty && nextLocusInRangeOpt.exists(!currentElementsOverlapLocus(_))) {
      nextLocusInRangeOpt = findNextNonEmptyLocus(nextLocusInRangeOpt.get)
    }
    nextLocusInRangeOpt
  }

  /**
   * @param locus Find next non-empty locus >= [[locus]]
   * @return Next non-empty locus >= [[locus]], or None if none exist
   */
  def findNextNonEmptyLocus(locus: Long): Option[Long] = {

    var nextLocusInRangeOpt: Option[Long] = Some(locus)
    // Find the next loci with overlapping elements that is in a valid range
    while (nextLocusInRangeOpt.exists(!nextElementOverlapsLocus(_))) {
      // Drop elements out of the window that are before the next locus in [[ranges]] and do not overlap
      windows.foreach(_.dropUntil(nextLocusInRangeOpt.get))

      // If any window has regions left find the minimum starting locus
      val nextLocusInWindows: Option[Long] = windows.flatMap(_.nextElement.map(_.start)).reduceOption(_ min _)

      // Find the next valid locus in ranges
      nextLocusInRangeOpt = nextLocusInWindows.flatMap(nextLocusInRange(_))
    }

    nextLocusInRangeOpt
  }

  /**
   *
   * Find the next locus is ranges that >= the given locus
   *
   * @param locus locus to find in ranges
   * @return The next locus >= [[locus]] which is contained in ranges
   */
  def nextLocusInRange(locus: Long): Option[Long] = {
    if (currentRange.end > locus) {
      Some(locus)
    } else if (ranges.hasNext) {
      currentRange = ranges.next()
      Some(currentRange.start)
    } else {
      None
    }
  }
}
