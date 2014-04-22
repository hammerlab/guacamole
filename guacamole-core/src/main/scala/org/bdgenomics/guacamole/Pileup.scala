package org.bdgenomics.guacamole

import org.bdgenomics.adam.rich.DecadentRead
import net.sf.samtools.{ Cigar, CigarOperator, TextCigarCodec }
import org.bdgenomics.adam.util.MdTag

/**
 * A [[Pileup]] at a locus contains a sequence of [[Pileup.Element]] instances, one for every read that overlaps that
 * locus. Each [[Pileup.Element]] specifies the base read at the given locus in a particular read. It also keeps track
 * of the read itself and the offset of the base in the read.
 *
 * @param elements Sequence of [[Pileup.Element]] instances giving the sequenced bases that align to a particular
 *                 reference locus, in arbitrary order.
 */
case class Pileup(elements: Seq[Pileup.Element]) {
  /** The first element in the pileup. */
  lazy val head = elements.head

  /** The reference locus that all the elements in this pileup align at. */
  lazy val locus: Long = head.locus

  /** The contig name for all elements in this pileup. */
  lazy val referenceName: String = head.read.record.referenceName.toString

  assume(elements.forall(_.read.record.referenceName == referenceName), "Reads in pileup have mismatching reference names")
  assume(elements.forall(_.locus == locus), "Reads in pileup have mismatching loci")

  /** The reference nucleotide base at this pileup's locus. */
  lazy val referenceBase: Char = {
    head.read.record.mdTag.get.getReference(head.read.record).charAt((head.locus - head.read.record.start).toInt)
  }

  /**
   * Split this [[Pileup]] by sample name. Returns a map from sample name to [[Pileup]] instances that use only reads
   * from that sample.
   */
  lazy val bySample: Map[String, Pileup] = {
    elements.groupBy(_.read.record.recordGroupSample.toString).map({
      case (sample, elements) => (sample, Pileup(elements))
    })
  }

  /**
   * Returns a new [[Pileup]] at a different locus on the same contig.
   *
   * To enable an efficient implementation, the new locus must be greater than the current locus.
   *
   * @param newLocus The locus to move forward to.
   * @param newReads The *new* reads, i.e. those that overlap the new locus, but not the current locus.
   * @return A new [[Pileup]] at the given locus.
   */
  def atGreaterLocus(newLocus: Long, newReads: Iterator[DecadentRead]) = {
    assume(elements.isEmpty || newLocus > locus, "New locus (%d) must be greater than current locus (%d)".format(newLocus, locus))
    val reusableElements = elements.filter(_.read.record.overlapsReferencePosition(newLocus).getOrElse(false))
    val updatedElements = reusableElements.map(_.atGreaterLocus(locus))
    val newElements = newReads.map(Pileup.Element(_, newLocus))
    Pileup(updatedElements ++ newElements)
  }

}
object Pileup {
  /**
   * Given an iterator over (locus, new reads) pairs, returns an iterator of [[Pileup]] instances at the given loci.
   *
   * @param locusAndReads Iterator of (locus, new reads) pairs, where "new reads" are those that overlap the current
   *                      locus, but not the previous locus.
   *
   * @return An iterator of [[Pileup]] instances at the given loci.
   */
  def pileupsAtLoci(locusAndReads: Iterator[(Long, Iterator[DecadentRead])]): Iterator[Pileup] = {
    val initialEmpty = Pileup(Seq.empty)
    val iterator = locusAndReads.scanLeft(initialEmpty)((prevPileup: Pileup, pair) => {
      val (locus, newReads) = pair
      prevPileup.atGreaterLocus(locus, newReads)
    })
    iterator.next() // Discard first element, the initial empty pileup.
    iterator
  }

  /**
   * Given reads and a locus, returns a [[Pileup]] at the specified locus.
   *
   * @param reads Sequence of reads, in any order, that may or may not overlap the locus.
   * @param locus The locus to return a [[Pileup]] at.
   * @return A [[Pileup]] at the given locus.
   */
  def apply(reads: Seq[DecadentRead], locus: Long): Pileup = {
    val elements = reads.filter(_.record.overlapsReferencePosition(locus).getOrElse(false)).map(Element(_, locus))
    Pileup(elements)
  }

  /**
   * A [[Pileup.Element]] represents a particular nucleotide sequenced by a particular read at a particular
   * reference locus.
   *
   * @param read The read this [[Element]] is coming from.
   * @param locus The reference locus.
   * @param readPosition The offset into the sequence of bases in the read that this element corresponds to.
   * @param cigar The parsed cigar object of this read.
   * @param indexInCigarElements Which cigar element in the read this element belongs to.
   * @param indexWithinCigarElement The offset of this element within the current cigar element
   */
  case class Element(
      read: DecadentRead,
      locus: Long,
      readPosition: Long,
      cigar: Cigar,
      indexInCigarElements: Long,
      indexWithinCigarElement: Long) {

    assume(locus >= read.record.getStart)
    assume(locus <= read.record.end.get)
    assume(read.record.mdTag.isDefined)

    /**
     * The sequenced nucleotides at this element. If the current element is a deletion, then is the empty string. If
     * it's an insertion, then this will be a string of length > 1. Otherwise, this is a string of length 1.
     */
    lazy val sequenceRead: String = {
      if (isDeletion)
        ""
      else if (isMatch || isMismatch)
        read.record.getSequence.charAt(readPosition.toInt).toString
      else if (isInsertion)
        read.record.getSequence.toString.subSequence(
          readPosition.toInt,
          readPosition.toInt + cigarElement.getLength - indexWithinCigarElement.toInt).toString
      else
        throw new AssertionError("Not a match, mismatch, deletion, or insertion")
    }
    lazy val singleBaseRead: Char = {
      assume(sequenceRead.length == 1)
      sequenceRead.charAt(0)
    }

    lazy val cigarElement = cigar.getCigarElement(indexInCigarElements.toInt)
    lazy val isInsertion = cigarElement == CigarOperator.INSERTION
    lazy val isDeletion = cigarElement == CigarOperator.DELETION
    lazy val isMismatch = cigarElement == CigarOperator.MATCH_OR_MISMATCH && !read.record.mdTag.get.isMatch(locus)
    lazy val isMatch = cigarElement == CigarOperator.MATCH_OR_MISMATCH && read.record.mdTag.get.isMatch(locus)

    /**
     * Returns a new [[Pileup.Element]] of the same read at a different locus.
     *
     * To enable an efficient implementation, newLocus must be greater than the current locus.
     *
     * @param newLocus The desired locus of the new [[Pileup.Element]]. It must be greater than the current locus, and
     *                 not past the end of the current read.
     *
     * @return A new [[Pileup.Element]] at the given locus.
     */
    def atGreaterLocus(newLocus: Long): Element = {
      if (newLocus == locus)
        this
      else {
        assume(newLocus > locus)
        assume(newLocus <= read.record.end.get)
        val desiredReferenceOffset = newLocus - read.record.getStart

        var currentReadPosition = readPosition - indexWithinCigarElement
        var currentReferenceOffset = locus - read.record.start - indexWithinCigarElement
        var cigarIndex = indexInCigarElements
        var prevReadPosition: Long = -1
        var prevReferenceOffset: Long = -1
        while (currentReferenceOffset < desiredReferenceOffset) {
          prevReadPosition = currentReadPosition
          prevReferenceOffset = currentReferenceOffset
          val element = cigar.getCigarElement(cigarIndex.toInt)
          if (element.getOperator.consumesReadBases) currentReadPosition += element.getLength
          if (element.getOperator.consumesReferenceBases) currentReferenceOffset += element.getLength
          cigarIndex += 1
        }
        assert(desiredReferenceOffset < currentReferenceOffset)
        assert(desiredReferenceOffset >= prevReferenceOffset)
        val newIndexInCigarElements = cigarIndex - 1
        val newIndexWithinCigarElement = desiredReferenceOffset - prevReferenceOffset
        val newReadPosition = prevReadPosition + newIndexWithinCigarElement
        Element(
          read,
          newLocus,
          newReadPosition,
          cigar,
          newIndexInCigarElements,
          newIndexWithinCigarElement)
      }
    }
  }
  object Element {
    /**
     * Create a new [[Pileup.Element]] backed by the given read at the specified locus. The read must overlap the locus.
     *
     */
    def apply(read: DecadentRead, locus: Long): Element = {
      val cigar = TextCigarCodec.getSingleton.decode(read.record.getCigar.toString)
      val mdTag = MdTag(read.record.getMismatchingPositions.toString, read.record.getStart)
      Element(
        read = read,
        locus = read.record.getStart,
        readPosition = 0,
        cigar = cigar,
        indexInCigarElements = 0,
        indexWithinCigarElement = 0).atGreaterLocus(locus)
    }
  }
}

