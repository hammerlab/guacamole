package org.hammerlab.guacamole.filters.pileup

import org.hammerlab.guacamole.pileup.PileupElement

/**
 * Filter to remove pileup elements close to edge of reads
 */
object EdgeBaseFilter {
  /**
   *
   * @param elements sequence of pileup elements to filter
   * @param minimumDistanceFromEndFromRead Threshold of distance from base to edge of read
   * @return filtered sequence of elements - those who were further from directional end minimumDistanceFromEndFromRead
   */
  def apply(elements: Seq[PileupElement], minimumDistanceFromEndFromRead: Int): Seq[PileupElement] =
    elements.filter(_.distanceFromSequencingEnd >= minimumDistanceFromEndFromRead)
}
