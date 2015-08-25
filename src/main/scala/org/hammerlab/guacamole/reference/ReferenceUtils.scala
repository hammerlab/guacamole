package org.hammerlab.guacamole.reference

import org.hammerlab.guacamole.Bases

object ReferenceUtils {

  /**
   * Compute the fraction of each base in the sequence
   * @param sequence Array of reference bases
   * @return Map of base to fraction
   */
  def getBaseFraction(sequence: Array[Byte]): Map[Byte, Float] = {
    val sequenceLength = sequence.length.toFloat
    sequence.groupBy(identity).mapValues(_.length / sequenceLength).withDefaultValue(0f)
  }

  /**
   * Compute the fraction of bases that are G or C in a reference sequence
   * @param sequence Array of reference bases
   * @return Fraction of G  C compared to all bases
   */
  def getGCFraction(sequence: Array[Byte]): Float = {
    val baseRatios = getBaseFraction(sequence)
    baseRatios(Bases.G) + baseRatios(Bases.C)
  }

  /**
   * Compute the fraction of bases that are A or T in a reference sequence
   * @param sequence Array of reference bases
   * @return Fraction of A  T compared to all bases
   */
  def getATFraction(sequence: Array[Byte]): Float = {
    val baseRatios = getBaseFraction(sequence)
    baseRatios(Bases.A) + baseRatios(Bases.T)
  }

}
