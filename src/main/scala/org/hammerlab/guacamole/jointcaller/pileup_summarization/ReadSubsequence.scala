package org.hammerlab.guacamole.jointcaller.pileup_summarization

import org.hammerlab.genomics.bases.Bases
import org.hammerlab.genomics.bases.Bases._
import org.hammerlab.genomics.reference.{ ContigSequence, Locus }
import org.hammerlab.guacamole.pileup.PileupElement
import org.hammerlab.guacamole.reads.MappedRead

/**
 * A sub-sequence of the bases sequenced by a MappedRead.
 *
 * @param read the mapped read
 * @param start reference locus of the first base in the sub-sequence
 * @param end one past the reference locus of the last base in the sub-sequence
 * @param length reference distance spanned by this subsequence, i.e. (end - start)
 * @param startReadPosition offset into the read of the first base in the sub-sequence
 * @param endReadPosition one past the offset into the read of last base in the sub-sequence
 */
case class ReadSubsequence(read: MappedRead,
                           start: Locus,
                           end: Locus,
                           length: Int,
                           startReadPosition: Int,
                           endReadPosition: Int) {

  assume(end > start)

  /** The sequenced bases as a string */
  def sequence: Bases = read.sequence.slice(startReadPosition, endReadPosition)

  /** true if the sequence contains only A,C,G,T (e.g. no N's) */
  def allStandardBases: Boolean = sequence.allStandardBases

  /** The base qualities corresponding to the sequenced bases. */
  def baseQualities: Seq[Int] =
    if (startReadPosition == endReadPosition)
      // Technically no sequenced bases at this location (deletion). Use quality of previous base.
      Seq(read.baseQualities(startReadPosition).toInt)
    else
      read.baseQualities.slice(startReadPosition, endReadPosition).map(_.toInt)

  /** Average base quality of the sequenced bases. */
  def meanBaseQuality: Double = {
    val qualities = baseQualities
    assert(qualities.nonEmpty)
    val result = qualities.sum.toDouble / qualities.length
    assert(result >= 0, "Invalid base qualities: %s".format(qualities.map(_.toString).mkString(" ")))
    result
  }

  /** The reference sequence at this locus. */
  def refSequence(contigReferenceSequence: ContigSequence): Bases =
    contigReferenceSequence.slice(start, length)
}

object ReadSubsequence {
  /**
   * Extract a sub-sequence of a particular length starting at a certain reference locus from a MappedRead.
   *
   * @param element PileupElement positioned at one reference base before the start of the desired sub-sequence
   * @param length reference length of the desired ReadSubsequence.
   * @return The sub-sequence of sequenced bases from element.read starting at reference position element.locus + 1
   *         and spanning reference length refSequence.length. If the base aligning to element.locus is non-reference
   *         base, or the read ends before the end of the reference region, the result is None.
   */
  def ofFixedReferenceLength(element: PileupElement, length: Int): Option[ReadSubsequence] = {
    assume(length > 0)

    if (element.allele.isVariant || element.locus.next >= element.read.end)
      None
    else {
      val firstElement = element.advanceToLocus(element.locus.next)
      var currentElement = firstElement
      var refLength = 1
      var next = currentElement.locus.next

      while (next < currentElement.read.end && refLength < length) {
        currentElement = currentElement.advanceToLocus(currentElement.locus.next)
        next = currentElement.locus.next
        refLength += 1
      }

      if (next >= currentElement.read.end)
        None
      else
        Some(
          ReadSubsequence(
            element.read,
            firstElement.locus,
            next,
            refLength,
            firstElement.readPosition,
            currentElement
              .advanceToLocus(next)
              .readPosition
          )
        )
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
   * @return ReadSubsequence for the alt allele, if one exists
   */
  def ofNextAltAllele(element: PileupElement): Option[ReadSubsequence] = {

    def isVariantOrFollowedByDeletion(e: PileupElement): Boolean = {
      e.allele.isVariant || (
        e.isFinalCigarBase && e.nextCigarElement.exists(
          cigar ⇒ !cigar.getOperator.consumesReadBases && cigar.getOperator.consumesReferenceBases))
    }

    if (isVariantOrFollowedByDeletion(element) || element.locus.next >= element.read.end)
      None
    else {
      val firstElement = element.advanceToLocus(element.locus.next)
      var currentElement = firstElement
      var refLength = 0

      while (currentElement.locus.next < currentElement.read.end && isVariantOrFollowedByDeletion(currentElement)) {
        currentElement = currentElement.advanceToLocus(currentElement.locus.next)
        refLength += 1
      }

      if (currentElement.locus == firstElement.locus || currentElement.locus == currentElement.read.end)
        // We either have no variant here, or we hit the end of the read.
        None
      else
        Some(
          ReadSubsequence(
            element.read,
            firstElement.locus,
            currentElement.locus,
            refLength,
            firstElement.readPosition,
            currentElement.readPosition
          )
        )
    }
  }

  /**
   * Given any number of pileup elements, return all alternate alleles starting at the elements' locus + 1.
   *
   * @param elements pileup element instances, should all be positioned at same locus
   * @return ReadSubsequence instances giving the non-reference sequenced alleles at this position
   */
  def nextAlts(elements: Seq[PileupElement]): Seq[ReadSubsequence] = {
    val startLocus = elements.headOption.map(_.locus)
    assume(elements.forall(_.locus == startLocus.get))
    elements.flatMap(element ⇒ ofNextAltAllele(element))
  }
}
