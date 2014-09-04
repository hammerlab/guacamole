package org.bdgenomics.guacamole.reads

import net.sf.samtools.{ SAMRecord, Cigar }
import org.bdgenomics.adam.util.{ PhredUtils, MdTag }
import org.bdgenomics.guacamole.{ LociSet, HasReferenceRegion }

import scala.collection.JavaConversions

/**
 * A mapped read. See the [[Read]] trait for some of the field descriptions.
 *
 * @param referenceContig the contig name (e.g. "chr12") that this read was mapped to.
 * @param alignmentQuality the mapping quality, phred scaled.
 * @param start the (0-based) reference locus that the first base in this read aligns to.
 * @param cigar parsed samtools CIGAR object.
 */
case class MappedRead(
    token: Int,
    sequence: Seq[Byte],
    baseQualities: Seq[Byte],
    isDuplicate: Boolean,
    sampleName: String,
    referenceContig: String,
    alignmentQuality: Int,
    start: Long,
    cigar: Cigar,
    mdTagString: String,
    failedVendorQualityChecks: Boolean,
    isPositiveStrand: Boolean,
    matePropertiesOpt: Option[MateProperties]) extends Read with HasReferenceRegion {

  assert(baseQualities.length == sequence.length,
    "Base qualities have length %d but sequence has length %d".format(baseQualities.length, sequence.length))

  final override lazy val getMappedReadOpt = Some(this)

  lazy val mdTag = MdTag(mdTagString, start)

  lazy val referenceString = mdTag.getReference(sequenceStr, cigar, start)

  lazy val alignmentLikelihood = PhredUtils.phredToSuccessProbability(alignmentQuality)

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
  val unclippedStart = cigarElements.takeWhile(Read.cigarElementIsClipped).foldLeft(start)({
    (pos, element) => pos - element.getLength
  })

  /**
   * The end of the read's alignment, including any final clipped bases, exclusive.
   */
  val unclippedEnd = cigarElements.reverse.takeWhile(Read.cigarElementIsClipped).foldLeft(end)({
    (pos, element) => pos + element.getLength
  })

}

case class MissingMDTagException(record: SAMRecord)
  extends Exception("Missing MDTag in SAMRecord: %s".format(record.toString))
