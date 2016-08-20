package org.hammerlab.guacamole.reads

import org.hammerlab.guacamole.readsets.{SampleId, SampleName}

/**
 * An unmapped read. See the [[Read]] trait for field descriptions.
 *
 */
case class UnmappedRead(
    name: String,
    sequence: IndexedSeq[Byte],
    baseQualities: IndexedSeq[Byte],
    isDuplicate: Boolean,
    sampleId: SampleId,
    sampleName: SampleName,
    failedVendorQualityChecks: Boolean,
    isPaired: Boolean) extends Read {

  assert(baseQualities.length == sequence.length)

  override val isMapped = false
  override def asMappedRead = None
}

