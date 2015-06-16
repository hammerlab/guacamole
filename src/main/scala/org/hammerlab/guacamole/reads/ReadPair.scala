package org.hammerlab.guacamole.reads

import htsjdk.samtools.SAMRecord

case class ReadPair[+T <: Read](read: T,
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

object ReadPair {
  def apply(record: SAMRecord,
            token: Int): ReadPair[Read] = {
    val read = Read.fromSAMRecordOpt(
      record,
      token,
      requireMDTagsOnMappedReads = true
    ).get

    val mateAlignment = MateAlignmentProperties(record)
    ReadPair(read, isFirstInPair = record.getFirstOfPairFlag, mateAlignment)
  }
}

