package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.loci.SimpleRange
import org.hammerlab.guacamole.reference.Locus

/**
 * An iterator over loci on a single contig. Loci from this iterator are sorted (monotonically increasing).
 *
 * This can be used as a plain scala Iterator[Long], but also supports extra functionality for quickly skipping
 * ahead past a given locus.
 *
 * @param loci loci to iterate over
 */
class ContigIterator(loci: Contig) extends scala.BufferedIterator[Locus] {
  private val ranges = loci.ranges.iterator

  /** The range for the next locus to be returned. */
  private var headRangeOption: Option[SimpleRange] = if (ranges.isEmpty) None else Some(ranges.next())

  /** The index in the range for the next locus. */
  private var headIndex = 0L

  /** true if calling next() will succeed. */
  def hasNext() = headRangeOption.nonEmpty

  /** The next element to be returned by next(). If the iterator is empty, throws NoSuchElementException. */
  def head: Long = headRangeOption match {
    case Some(range) => range.start + headIndex
    case None        => throw new NoSuchElementException("empty iterator")
  }

  /**
   * Advance the iterator and return the current head.
   *
   * Throws NoSuchElementException if the iterator is already at the end.
   */
  def next(): Locus = {
    val nextLocus: Long = head // may throw

    // Advance
    headIndex += 1
    if (headIndex == headRangeOption.get.length) {
      nextRange()
    }
    nextLocus
  }

  /**
   * Skip ahead to the first locus in the iterator that is at or past the given locus.
   *
   * After calling this, a subsequent call to next() will return the first element in the iterator that is >= the
   * given locus. If there is no such element, then the iterator will be empty after calling this method.
   *
   */
  def skipTo(locus: Locus): Unit = {
    // Skip entire ranges until we hit one whose end is past the target locus.
    while (headRangeOption.exists(_.end <= locus)) {
      nextRange()
    }
    // If we're not at the end of the iterator and the current head range includes the target locus, set our index
    // so that our next locus is the target.
    headRangeOption match {
      case Some(range) if (locus >= range.start && locus < range.end) => {
        headIndex = locus - range.start
      }
      case _ => {}
    }
  }

  /**
   * Advance past all loci in the current head range. Return the next range if one exists.
   */
  private def nextRange(): Option[SimpleRange] = {
    headIndex = 0
    headRangeOption = if (ranges.hasNext) Some(ranges.next()) else None
    headRangeOption
  }
}
