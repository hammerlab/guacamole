package org.bdgenomics.guacamole

/**
 *
 * This iterator moves locus by locus through a collection of sliding windows.
 *
 * Each call to next advances each SlidingWindow 1 locus (unless skipEmpty is set where it advances to the next non-empty
 * locus.
 *
 * @param ranges Collection of ranges the iterator should cover
 * @param skipEmpty Skip loci if the windows contain no overlapping regions
 * @param restWindows Sliding Window of regions
 */
case class SlidingWindowsIterator[Region <: HasReferenceRegion](ranges: Iterator[LociMap.SimpleRange],
                                                                skipEmpty: Boolean,
                                                                headWindow: SlidingWindow[Region],
                                                                restWindows: Seq[SlidingWindow[Region]]) extends Iterator[Seq[SlidingWindow[Region]]] {

  private val windows: Seq[SlidingWindow[Region]] = headWindow :: restWindows.toList

  private var currentRange: Option[LociMap.SimpleRange] = None
  private var currentLocus: Option[Long] = None

  private val halfWindowSize = headWindow.halfWindowSize
  private def windowsEmpty = windows.forall(_.currentRegions.isEmpty)

  /**
   * This advances the sliding window to the next locus if it exists, and if skipEmpty is true the window will be moved
   * to the next non-empty locus
   *
   * @return true if there are locus left to process and if skipEmpty is true, there is a locus with overlapping elements
   */
  override def hasNext: Boolean = {
    if (skipEmpty) updateNextNonEmptyLocus() else updateNextLocus()
    currentLocus.isDefined
  }

  /**
   *
   * Next returns the sequences of windows we are iterating over, which have been advanced to the next locus
   * The window cover a region of (2 * halfWindowSize + 1) around the currentLocus
   * and contain all of the elements that overlap that region
   *
   * Each call to next() returns windows that been advanced to the next locus (and corresponding overlapping reads are adjusted)
   *
   * @return The sliding windows advanced to the next locus (next nonEmpty locus if skipEmpty is true)
   */
  override def next(): Seq[SlidingWindow[Region]] = {
    windows
  }

  private def updateNextLocus() = {
    (currentRange, currentLocus) match {
      // Increment locus if within current range
      case (Some(range), Some(locus)) if range.end > locus + 1 => {
        currentLocus = Some(locus + 1)
        windows.foreach(_.setCurrentLocus(locus + 1))
      }
      // If locus exceeds the range, advance to the next range
      case _ if ranges.hasNext => {
        val nextRange = ranges.next()
        currentRange = Some(nextRange)
        currentLocus = Some(nextRange.start)
        windows.foreach(_.setCurrentLocus(nextRange.start))
      }
      // No loci left to process
      case _ =>  currentLocus = None
    }
  }

  private def updateNextNonEmptyLocus() = {
    updateNextLocus()
    lazy val nextReadStartOpt = firstStartLocus(windows:_*)
    currentLocus match {
      // Fast forward if the next read start it outside of the window
      case Some(locus) if windowsEmpty && nextReadStartOpt.exists( _ - halfWindowSize > locus)  => {
        windows.foreach(_.setCurrentLocus(nextReadStartOpt.get))
        currentLocus = nextReadStartOpt
      }
      case _ => {
        // Find the next locus in the range with overlapping elements
        while (windowsEmpty && currentLocus.isDefined) {
          updateNextLocus()
        }
      }
    }
  }

  /**
   * Helper function. Given some sliding window instances, return the lowest nextStartLocus from any of them. If all of
   * the sliding windows are at the end of the region iterators, return Long.MaxValue.
   */
  private def firstStartLocus(windows: SlidingWindow[Region]*) = {
    val starts = windows.map(_.nextStartLocus)
    starts.min
  }
}
