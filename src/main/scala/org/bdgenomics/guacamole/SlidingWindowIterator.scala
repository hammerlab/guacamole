package org.bdgenomics.guacamole

/**
 * @param ranges Collection of ranges the iterator should cover
 * @param skipEmpty Skip loci if the windows contain no overlapping regions
 * @param restWindows Sliding Window of regions
 */
case class SlidingWindowsIterator[Region <: HasReferenceRegion](ranges: Iterator[LociMap.SimpleRange],
                                                                skipEmpty: Boolean,
                                                                headWindow: SlidingWindow[Region],
                                                                restWindows: SlidingWindow[Region]*) extends Iterator[Seq[SlidingWindow[Region]]] {

  val windows: Seq[SlidingWindow[Region]] = headWindow :: restWindows.toList

  var currentRange: Option[LociMap.SimpleRange] = None
  var currentLocus: Option[Long] = None

  override def hasNext: Boolean = {
    if (skipEmpty) updateNextNonEmptyLocus() else updateNextLocus()
    currentLocus.isDefined
  }

  val halfWindowSize = headWindow.halfWindowSize
  def windowsEmpty = windows.forall(_.currentRegions.isEmpty)

  override def next(): Seq[SlidingWindow[Region]] = {
    windows
  }

  def updateNextLocus() = {
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

  def updateNextNonEmptyLocus() = {
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
  def firstStartLocus(windows: SlidingWindow[Region]*) = {
    val starts = windows.map(_.nextStartLocus)
    starts.min
  }
}
