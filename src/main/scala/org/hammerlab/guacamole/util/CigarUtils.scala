package org.hammerlab.guacamole.util

import htsjdk.samtools.{CigarElement, CigarOperator}

object CigarUtils {
  /**
   * The length of a cigar element in read coordinate space.
   *
   * @param element Cigar Element
   * @return Length of CigarElement, 0 if it does not consume any read bases.
   */
  def getReadLength(element: CigarElement): Int = {
    if (element.getOperator.consumesReadBases) element.getLength else 0
  }

  /**
   * The length of a cigar element in reference coordinate space.
   *
   * @param element Cigar Element
   * @return Length of CigarElement, 0 if it does not consume any reference bases.
   */
  def getReferenceLength(element: CigarElement): Int = {
    if (element.getOperator.consumesReferenceBases) element.getLength else 0
  }

  /** Is the given samtools CigarElement a (hard/soft) clip? */
  def isClipped(element: CigarElement): Boolean = {
    element.getOperator == CigarOperator.SOFT_CLIP || element.getOperator == CigarOperator.HARD_CLIP
  }
}
