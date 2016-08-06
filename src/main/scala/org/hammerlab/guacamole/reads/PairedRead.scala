package org.hammerlab.guacamole.reads

import org.hammerlab.guacamole.readsets.{SampleId, SampleName}

/**
 * PairedRead is a MappedRead or UnmappedRead with the additional mate information
 *
 * @param read Unmapped or MappedRead base read
 * @param isFirstInPair Whether the read is earlier that the the mate read
 * @param mateAlignmentProperties Alignment location of the mate if it the mate is aligned
 * @tparam T UnmappedRead or MappedRead
 */
case class PairedRead[+T <: Read](read: T,
                                  isFirstInPair: Boolean,
                                  mateAlignmentProperties: Option[MateAlignmentProperties]) extends Read {

  val isMateMapped = mateAlignmentProperties.isDefined
  override val name: String = read.name
  override val failedVendorQualityChecks: Boolean = read.failedVendorQualityChecks
  override val sampleId: SampleId = read.sampleId
  override val sampleName: SampleName = read.sampleName
  override val baseQualities: IndexedSeq[Byte] = read.baseQualities
  override val isDuplicate: Boolean = read.isDuplicate
  override val sequence: IndexedSeq[Byte] = read.sequence
  override val isPaired: Boolean = true
  override val isMapped = read.isMapped
  override def asMappedRead = read.asMappedRead
}
