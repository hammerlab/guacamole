package org.hammerlab.guacamole.reads

import htsjdk.samtools.{Cigar, CigarElement}
import org.bdgenomics.adam.util.PhredUtils.phredToSuccessProbability
import org.hammerlab.guacamole.pileup.PileupElement
import org.hammerlab.guacamole.readsets.SampleId
import org.hammerlab.guacamole.reference.{ContigName, ContigSequence, Locus, ReferenceRegion}
import org.hammerlab.guacamole.util.CigarUtils
import org.hammerlab.guacamole.util.Bases.basesToString

import scala.collection.JavaConversions

/**
 * A mapped read. See the [[Read]] trait for some of the field descriptions.
 *
 * @param contigName the contig name (e.g. "chr12") that this read was mapped to.
 * @param alignmentQuality the mapping quality, phred scaled.
 * @param start the (0-based) reference locus that the first base in this read aligns to.
 * @param cigar parsed samtools CIGAR object.
 */
case class MappedRead(
    name: String,
    sequence: IndexedSeq[Byte],
    baseQualities: IndexedSeq[Byte],
    isDuplicate: Boolean,
    sampleId: SampleId,
    contigName: ContigName,
    alignmentQuality: Int,
    start: Locus,
    cigar: Cigar,
    failedVendorQualityChecks: Boolean,
    isPositiveStrand: Boolean,
    isPaired: Boolean)
  extends Read

    with ReferenceRegion {

  assert(baseQualities.length == sequence.length,
    "Base qualities have length %d but sequence has length %d".format(baseQualities.length, sequence.length))

  override val isMapped = true
  override def asMappedRead = Some(this)


  /**
   * Number of mismatching bases in this read. Does *not* include indels: only looks at read bases that align to a
   * single base in the reference and do not match it.
   *
   * @param contigSequence the reference sequence for this read's contig
   * @return count of mismatching bases
   */
  private var cachedCountOfMismatches = -1
  def countOfMismatches(contigSequence: ContigSequence): Int = {
    if (cachedCountOfMismatches == -1) {
      var element = PileupElement(this, start, contigSequence)
      var count = (if (element.isMismatch) 1 else 0)
      while (element.locus < end - 1) {
        element = element.advanceToLocus(element.locus + 1)
        count += (if (element.isMismatch) 1 else 0)
      }
      cachedCountOfMismatches = count
    }
    cachedCountOfMismatches
  }

  lazy val alignmentLikelihood = phredToSuccessProbability(alignmentQuality)

  /** Individual components of the CIGAR string (e.g. "10M"), parsed, and as a Scala buffer. */
  val cigarElements = JavaConversions.asScalaBuffer(cigar.getCigarElements)

  /**
   * The end of the alignment, exclusive. This is the first reference locus AFTER the locus corresponding to the last
   * base in this read.
   */
  val end: Long = start + cigar.getPaddedReferenceLength

  /**
   * A read can be "clipped", meaning that some prefix or suffix of it did not align. This is the start of the whole
   * read's alignment, including any initial clipped bases.
   */
  val unclippedStart = cigarElements.takeWhile(CigarUtils.isClipped).foldLeft(start)({
    (pos, element) => pos - element.getLength
  })

  /**
   * The end of the read's alignment, including any final clipped bases, exclusive.
   */
  val unclippedEnd = cigarElements.reverse.takeWhile(CigarUtils.isClipped).foldLeft(end)({
    (pos, element) => pos + element.getLength
  })

  /**
   * Take a subsequence or slice of a read [from, until)
   * Returns None if the provided coordinates do not overlap the read
   *
   * @param from reference locus to start the new read
   * @param until reference locus to slice up to (exclusive end)
   * @param contigSequence reference sequence for the read's contig
   * @return A read which spans [from, until) or None if the provided coordinates do not overlap the read
   */
  def slice(from: Long, until: Long, contigSequence: ContigSequence): Option[MappedRead] = {
    if (from >= end || until < start) {
      None
    } else if (from <= start && until >= end) {
      Some(this)
    } else {

      val referenceStart = math.max(from, start)
      val referenceEnd = math.min(until - 1, end - 1)

      val el = PileupElement(this, start, contigSequence)
      val startElement = el.advanceToLocus(referenceStart)
      val readStartIndex = startElement.readPosition
      val startCigarIndex = startElement.cigarElementIndex
      val startIndexWithinCigarElement = startElement.indexWithinCigarElement
      val startCigarElement = cigarElements(startCigarIndex)
      val slicedStartCigar = new CigarElement(startCigarElement.getLength - startIndexWithinCigarElement, startCigarElement.getOperator)

      val endElement = el.advanceToLocus(referenceEnd)
      val readEndIndex = endElement.readPosition
      val endCigarIndex = endElement.cigarElementIndex
      val endIndexWithinCigarElement = endElement.indexWithinCigarElement
      val endCigarElement = cigarElements(endCigarIndex)

      val slicedSequence = sequence.slice(readStartIndex, readEndIndex + 1)
      val slicedBaseQualities = baseQualities.slice(readStartIndex, readEndIndex + 1)

      val slicedCigar =
        if (endCigarIndex == startCigarIndex)
          new Cigar(JavaConversions.seqAsJavaList(
            Seq(new CigarElement(endIndexWithinCigarElement - startIndexWithinCigarElement + 1, startCigarElement.getOperator))
          ))
        else
          new Cigar(JavaConversions.seqAsJavaList(
            Seq(slicedStartCigar) ++
              cigarElements.slice(startCigarIndex + 1, endCigarIndex) ++
              Seq(new CigarElement(endIndexWithinCigarElement + 1, endCigarElement.getOperator)))
          )

      Some(this.copy(
        sequence = slicedSequence,
        baseQualities = slicedBaseQualities,
        start = referenceStart,
        cigar = slicedCigar
      ))
    }
  }

  override def toString: String =
    "MappedRead(%s:%d, %s, %s)".format(
      contigName, start,
      cigar.toString,
      basesToString(sequence)
    )
}
