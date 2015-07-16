package org.hammerlab.guacamole.reads

import htsjdk.samtools.SAMRecord

/**
 * PairedRead is a MappedRead or UnmappedRead with the additional mate information
 * @param read Unmapped or MappedRead base read
 * @param isFirstInPair Whether the read is earlier that the the mate read
 * @param mateAlignmentProperties Alignment location of the mate if it the mate is aligned
 * @tparam T UnmappedRead or MappedRead
 */
case class PairedRead[+T <: Read](read: T,
                                  isFirstInPair: Boolean,
                                  mateAlignmentProperties: Option[MateAlignmentProperties]) extends Read {

  val isMateMapped = mateAlignmentProperties.isDefined
  override val token: Int = read.token
  override val failedVendorQualityChecks: Boolean = read.failedVendorQualityChecks
  override val sampleName: String = read.sampleName
  override val isPositiveStrand: Boolean = read.isPositiveStrand
  override val baseQualities: Seq[Byte] = read.baseQualities
  override val isDuplicate: Boolean = read.isDuplicate
  override val sequence: Seq[Byte] = read.sequence
  override val isPaired: Boolean = true
}

object PairedRead {
  def apply(record: SAMRecord,
            token: Int,
            requireMDTagsOnMappedReads: Boolean): Option[PairedRead[Read]] = {
    val read = Read.fromSAMRecordOpt(
      record,
      token,
      requireMDTagsOnMappedReads
    )

    val mateAlignment = MateAlignmentProperties(record)
    read.map(PairedRead(_, isFirstInPair = record.getFirstOfPairFlag, mateAlignment))
  }
}

