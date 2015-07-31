package org.hammerlab.guacamole.reads

import htsjdk.samtools.Cigar
import org.bdgenomics.adam.util.MdTag
import org.hammerlab.guacamole.Bases

class MDTaggedRead(token: Int,
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
                   isPaired: Boolean) extends MappedRead(
  token,
  sequence,
  baseQualities,
  isDuplicate,
  sampleName,
  referenceContig,
  alignmentQuality,
  start,
  cigar,
  mdTagString = Some(mdTagString),
  failedVendorQualityChecks,
  isPositiveStrand,
  isPaired) {

  def mdTag = MdTag(mdTagString, start)

  lazy val referenceBases: Seq[Byte] =
    try {
      MDTagUtils.getReference(mdTag, sequence, cigar, allowNBase = true)
    } catch {
      case e: IllegalStateException => throw new CigarMDTagMismatchException(cigar, mdTag, e)
    }

  /**
   * Find the reference base at a given locus for any locus that overlaps this read sequence
   *
   * @param referenceLocus Locus (0-based) at which to look up the reference base
   * @return  The reference base at the given locus
   */
  def getReferenceBaseAtLocus(referenceLocus: Long): Byte = {
    assume(referenceLocus >= start && referenceLocus < end)
    referenceBases((referenceLocus - start).toInt)
  }

  override def toString(): String =
    "MDTaggedRead(%s:%d, %s, %s, %s)".format(
      referenceContig, start,
      cigar.toString,
      mdTagString,
      Bases.basesToString(sequence)
    )
}
