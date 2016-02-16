package org.hammerlab.guacamole.commands.jointcaller

import org.apache.spark.Logging
import org.hammerlab.guacamole.Bases
import org.hammerlab.guacamole.pileup.PileupElement
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.reference.ContigSequence

/**
 * A sub-sequence of the bases sequenced by a MappedRead.
 *
 * @param read the mapped read
 * @param startLocus reference locus of the first base in the sub-sequence
 * @param endLocus one past the reference locus of the last base in the sub-sequence
 * @param startReadPosition offset into the read of the first base in the sub-sequence
 * @param endReadPosition one past the offset into the read of last base in the sub-sequence
 */
case class ReadSubsequence(read: MappedRead,
                           startLocus: Long,
                           endLocus: Long,
                           startReadPosition: Int,
                           endReadPosition: Int) extends Logging {

  assume(endLocus > startLocus)

  /** Number of reference bases spanned */
  def referenceLength: Int = (endLocus - startLocus).toInt

  /** The sequenced bases as a string */
  def sequence(): String = Bases.basesToString(read.sequence.slice(startReadPosition, endReadPosition))

  /** The base qualities corresponding to the sequenced bases. */
  def baseQualities(): Seq[Int] = read.baseQualities.slice(startReadPosition, endReadPosition).map(_.toInt)

  /** Average base quality of the sequenced bases. */
  def meanBaseQuality(): Double = {
    val qualities = baseQualities
    val result = qualities.sum.toDouble / qualities.length
    if (result < 0) {
      log.warn("Negative mean base qualities: %f [%s]".format(result, baseQualities.map(_.toString).mkString(" ")))
      10
    } else {
      result
    }
  }

  /** The reference sequence at this locus. */
  def refSequence(contigReferenceSequence: ContigSequence): String = {
    Bases.basesToString(contigReferenceSequence.slice(startLocus.toInt, endLocus.toInt))
  }
}
object ReadSubsequence {
  /**
   * Extract a sub-sequence of a particular length starting at a certain reference locus from a MappedRead.
   *
   * @param element PileupElement positioned at one reference base before the start of the desired sub-sequence
   * @param refSequence reference bases. First element is the reference base immediately following the PileupElement's
   *                    locus. The length determines the reference length of the resulting ReadSubsequence.
   * @return The sub-sequence of sequenced bases from element.read starting at reference position element.locus + 1
   *         and spanning reference length refSequence.length. If the base aligning to element.locus is non-reference
   *         base, or the read ends before the end of the reference region, the result is None.
   */
  def ofFixedReferenceLength(element: PileupElement, refSequence: Seq[Byte]): Option[ReadSubsequence] = {
    assume(refSequence.nonEmpty)

    if (element.allele.isVariant || element.locus >= element.read.end - 1) {
      None
    } else {
      val firstElement = element.advanceToLocus(element.locus + 1, refSequence(0))
      var currentElement = firstElement
      var refOffset = 1
      while (currentElement.locus < currentElement.read.end - 1 && refOffset < refSequence.length) {
        currentElement = currentElement.advanceToLocus(currentElement.locus + 1, refSequence(refOffset))
        refOffset += 1
      }
      if (currentElement.locus == currentElement.read.end) {
        None
      } else {
        Some(
          ReadSubsequence(
            element.read,
            firstElement.locus,
            currentElement.locus + 1,
            firstElement.readPosition,
            currentElement.readPosition + 1))
      }
    }
  }

  /**
   * Extract a sub-sequence containing an alternate allele, if there is one, from a PileupElement's subsequent position.
   *
   * If the element is currently NOT positioned at an alt, but the *following* base is an alt, then return a
   * ReadSubsequence comprising the consecutive alternate sequenced bases, which could be any length. If the element's
   * current position is at a non-reference-matching base, or the subsequent bases match the reference, or the read ends
   * before a reference-matching base occurs, then return None.
   *
   * @param element PileupElement positioned at one reference base before the start of the desired sub-sequence
   * @param contigReferenceSequence reference for the entire contig
   * @return ReadSubsequence for the alt allele, if one exists
   */
  def ofNextAltAllele(element: PileupElement, contigReferenceSequence: ContigSequence): Option[ReadSubsequence] = {
    if (element.allele.isVariant || element.locus >= element.read.end - 1) {
      None
    } else {
      val firstElement = element.advanceToLocus(element.locus + 1, contigReferenceSequence(element.locus.toInt + 1))
      var currentElement = firstElement
      while (currentElement.locus < currentElement.read.end - 1 && currentElement.allele.isVariant) {
        currentElement = currentElement.advanceToLocus(
          currentElement.locus + 1,
          contigReferenceSequence(currentElement.locus.toInt + 1))
      }
      if (currentElement.locus == firstElement.locus || currentElement.locus == currentElement.read.end) {
        // We either have no variant here, or we hit the end of the read.
        None
      } else {
        Some(
          ReadSubsequence(
            element.read,
            firstElement.locus,
            currentElement.locus,
            firstElement.readPosition,
            currentElement.readPosition))
      }
    }
  }

  /**
   * Given any number of pileup elements, return all alternate alleles starting at the elements' locus + 1.
   *
   * @param elements pileup element instances, should all be positioned at same locus
   * @param contigReferenceSequence reference sequence for entire contig
   * @return ReadSubsequence instances giving the non-reference sequenced alleles at this position
   */
  def nextAlts(elements: Seq[PileupElement], contigReferenceSequence: ContigSequence): Seq[ReadSubsequence] = {
    val startLocus = elements.headOption.map(_.locus)
    assume(elements.forall(_.locus == startLocus.get))
    elements.flatMap(element => ofNextAltAllele(element, contigReferenceSequence))
  }
}