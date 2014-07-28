package org.bdgenomics.guacamole.filters

import org.bdgenomics.guacamole.pileup.PileupElement
/**
 * Filter to remove pileup elements with low alignment quality
 */
object QualityAlignedReadsFilter {
  /**
   *
   * @param elements sequence of pileup elements to filter
   * @param minimumAlignmentQuality Threshold to define whether a read was poorly aligned
   * @return filtered sequence of elements - those who had higher than minimumAlignmentQuality alignmentQuality
   */
  def apply(elements: Seq[PileupElement], minimumAlignmentQuality: Int): Seq[PileupElement] = {
    elements.filter(_.read.alignmentQuality >= minimumAlignmentQuality)
  }

}

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
  def apply(elements: Seq[PileupElement], minimumDistanceFromEndFromRead: Int): Seq[PileupElement] = {
    elements.filter(_.distanceFromSequencingEnd >= minimumDistanceFromEndFromRead)
  }
}

