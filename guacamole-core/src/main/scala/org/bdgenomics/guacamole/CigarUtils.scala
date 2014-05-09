package org.bdgenomics.guacamole

import net.sf.samtools.CigarElement

object CigarUtils {

  /**
   * Compute the length of a cigar element in read coordinate space
   *
   * @param element Cigar Element
   * @return Length of CigarElement, 0 if it does not consume any read bases
   */
  def getReadLength(element: CigarElement): Int = {
    if (element.getOperator.consumesReadBases()) {
      element.getLength
    } else {
      0
    }
  }

  /**
   * Compute the length of a cigar element in reference coordinate space
   *
   * @param element Cigar Element
   * @return Length of CigarElement, 0 if it does not consume any reference bases
   */
  def getReferenceLength(element: CigarElement): Int = {
    if (element.getOperator.consumesReferenceBases()) {
      element.getLength
    } else {
      0
    }
  }

}
