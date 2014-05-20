package org.bdgenomics.guacamole.pileup

import org.bdgenomics.adam.rich.DecadentRead
import net.sf.samtools.{ TextCigarCodec, CigarOperator, CigarElement }
import org.bdgenomics.guacamole.CigarUtils
import scala.annotation.tailrec
import scala.collection.JavaConversions._

/**
 * A [[PileupElement]] represents the bases  sequenced by a particular read at a particular reference locus.
 *
 * @param read The read this [[PileupElement]] is coming from.
 * @param locus The reference locus.
 * @param readPosition The offset into the sequence of bases in the read that this element corresponds to.
 * @param remainingReadCigar A list of remaining parsed cigar elements of this read.
 * @param cigarElementLocus The reference START position of the cigar element.
 *                          If the element is an INSERTION this the PRECEDING reference base
 * @param indexInCigarElements Which cigar element in the read this element belongs to.
 * @param indexWithinCigarElement The offset of this element within the current cigar element.
 */
case class PileupElement(
    read: DecadentRead,
    locus: Long,
    readPosition: Long,
    remainingReadCigar: List[CigarElement],
    cigarElementLocus: Long,
    indexInCigarElements: Long,
    indexWithinCigarElement: Long) {

  assume(locus >= read.record.getStart)
  assume(locus < read.record.end.get)
  assume(read.record.mdTag.isDefined, "Record has no MDTag.")

  lazy val cigarElement = remainingReadCigar.head
  lazy val nextCigarElement = if (remainingReadCigar.tail.isEmpty) None else Some(remainingReadCigar.tail.head)

  /*
   * True if this is the last base of the current cigar element.
   */
  def isFinalCigarBase: Boolean = indexWithinCigarElement == cigarElement.getLength - 1

  lazy val alignment: Alignment = {
    val cigarOperator = cigarElement.getOperator
    val nextBaseCigarElement = if (isFinalCigarBase) nextCigarElement else Some(cigarElement)
    val nextBaseCigarOperator = nextBaseCigarElement.map(_.getOperator)
    (cigarOperator, nextBaseCigarOperator) match {
      // Since insertions by definition have no corresponding reference loci, there is a choice in whether we "attach"
      // them to the preceding or following locus. Here we attach them to the preceding base, since that seems to be the
      // conventional choice. That is, if we have a match followed by an insertion, the final base of the match will
      // get combined with the insertion into one Alignment, at the match's reference locus.
      case (CigarOperator.M, Some(CigarOperator.I)) | (CigarOperator.EQ, Some(CigarOperator.I)) | (CigarOperator.I, _) =>
        val startReadOffset: Int = readPosition.toInt
        val endReadOffset: Int = readPosition.toInt + CigarUtils.getReadLength(nextBaseCigarElement.get) + 1
        val bases = read.record.getSequence.subSequence(startReadOffset, endReadOffset).toString
        val qualities = read.record.qualityScores.slice(startReadOffset, endReadOffset)
        Insertion(bases, qualities)
      case (CigarOperator.M, _) | (CigarOperator.EQ, _) | (CigarOperator.X, _) =>
        val base: Char = read.record.getSequence.charAt(readPosition.toInt)
        val quality = read.record.qualityScores(readPosition.toInt)
        if (read.record.mdTag.get.isMatch(locus)) {
          Match(base, quality)
        } else {
          Mismatch(base, quality)
        }
      case (CigarOperator.D, _) | (CigarOperator.S, _) | (CigarOperator.N, _) | (CigarOperator.H, _) => Deletion()
      case (CigarOperator.P, _) =>
        throw new AssertionError("`P` CIGAR-ops should have been ignored earlier in `findNextCigarElement`")
    }
  }

  /* If you only care about what kind of CigarOperator is at this position, but not its associated sequence, then you
   * can use these state variables.
   */
  lazy val isInsertion = alignment match { case Insertion(_, _) => true; case _ => false }
  lazy val isDeletion = alignment match { case Deletion() => true; case _ => false }
  lazy val isMismatch = alignment match { case Mismatch(_, _) => true; case _ => false }
  lazy val isMatch = alignment match { case Match(_, _) => true; case _ => false }

  /**
   * The sequenced nucleotides at this element.
   *
   * If the current element is a deletion, then this is the empty string. If it's
   * an insertion, then this will be a string of length >= 1: the contents of
   * the inserted sequence starting at the current locus. Otherwise, this is
   * a string of length 1.
   */
  lazy val sequencedBases: String = alignment match {
    case Deletion()          => ""
    case Match(base, _)      => base.toString
    case Mismatch(base, _)   => base.toString
    case Insertion(bases, _) => bases
  }

  /**
   * For matches, mismatches, and single base insertions, this is the base sequenced at this locus, as a char. For
   * all other cases, this throws an assertion error.
   */
  lazy val sequencedSingleBase: Char = alignment match {
    case Match(base, _)                           => base
    case Mismatch(base, _)                        => base
    case Insertion(bases, _) if bases.length == 1 => bases.charAt(0)
    case other =>
      throw new AssertionError("Not a match, mismatch, or single nucleotide insertion: " + other.toString)
  }

  /*
   * Base quality score, phred-scaled.
   *
   * For matches and mismatches this is the base quality score of the current base.
   * For insertions this the minimum base quality score of all the bases in the insertion.
   * For deletions this is the mapping quality as there are no base quality scores available.
   */
  lazy val qualityScore: Int = alignment match {
    case Deletion()                  => read.record.getMapq
    case Match(_, qualityScore)      => qualityScore
    case Mismatch(_, qualityScore)   => qualityScore
    case Insertion(_, qualityScores) => qualityScores.min
  }

  /**
   * Returns a new [[PileupElement]] of the same read at a different locus.
   *
   * To enable an efficient implementation, newLocus must be greater than the current locus.
   *
   * @param newLocus The desired locus of the new [[PileupElement]]. It must be greater than the current locus, and
   *                 not past the end of the current read.
   *
   * @return A new [[PileupElement]] at the given locus.
   */
  def elementAtGreaterLocus(newLocus: Long): PileupElement = {
    if (newLocus == locus) { return this }

    assume(newLocus > locus, "Can't rewind to locus %d from %d. Pileups only advance.".format(newLocus, locus))
    val readEndPos = read.record.end.get
    assume(newLocus < readEndPos, "This read stops at position %d. Can't advance to %d".format(readEndPos, newLocus))

    val currentCigarReadPosition = if (cigarElement.getOperator.consumesReadBases()) {
      readPosition - indexWithinCigarElement
    } else {
      readPosition
    }
    // Iterate through the remaining cigar elements to find one overlapping the current position.
    @tailrec
    def getCurrentElement(remainingCigarElements: List[CigarElement], cigarReadPosition: Long, cigarReferencePosition: Long, cigarElementIndex: Long): PileupElement = {
      if (remainingCigarElements.isEmpty) {
        throw new RuntimeException(
          "Couldn't find cigar element for locus %d, cigar string only extends to %d".format(newLocus, cigarReferencePosition))
      }
      val nextCigarElement = remainingCigarElements.head
      val cigarOperator = nextCigarElement.getOperator
      val cigarElementReadLength = CigarUtils.getReadLength(nextCigarElement)
      val cigarElementReferenceLength = CigarUtils.getReferenceLength(nextCigarElement)
      // The 'P' (padding) operator is used to indicate a deletion-in-an-insertion. This only comes up when the
      // aligner attempted not only to align reads to the reference, but also to align inserted sequences within reads
      // to each other. In particular, a de novo assembler would automatically be doing this.
      // We ignore this operator, since our simple Alignment handling code does not expose within-insertion alignments.
      // See: http://davetang.org/wiki/tiki-index.php?page=SAM
      if (cigarOperator != CigarOperator.P) {
        val currentElementEnd = cigarReferencePosition + cigarElementReferenceLength
        if (currentElementEnd > newLocus) {
          val offset = newLocus - cigarReferencePosition
          val finalReadPos = if (cigarOperator.consumesReadBases) cigarReadPosition + offset else cigarReadPosition
          return PileupElement(
            read,
            newLocus,
            finalReadPos,
            remainingCigarElements,
            cigarReferencePosition,
            cigarElementIndex,
            offset)
        }
      }
      getCurrentElement(remainingCigarElements.tail, cigarReadPosition + cigarElementReadLength, cigarReferencePosition + cigarElementReferenceLength, cigarElementIndex + 1)
    }

    getCurrentElement(remainingReadCigar, currentCigarReadPosition, cigarElementLocus, indexInCigarElements)
  }
}

object PileupElement {
  /**
   * Create a new [[PileupElement]] backed by the given read at the specified locus. The read must overlap the locus.
   */
  def apply(read: DecadentRead, locus: Long): PileupElement = {
    assume(read.isAligned)
    assume(locus >= read.record.start)
    assume(read.record.end.isDefined)
    assume(locus < read.record.end.get)

    val cigar = TextCigarCodec.getSingleton.decode(read.record.getCigar.toString).getCigarElements.toList
    val startElement = PileupElement(
      read = read,
      locus = read.record.getStart,
      readPosition = 0,
      remainingReadCigar = cigar,
      cigarElementLocus = read.record.getStart,
      indexInCigarElements = 0,
      indexWithinCigarElement = 0)
    startElement.elementAtGreaterLocus(locus)
  }
}
