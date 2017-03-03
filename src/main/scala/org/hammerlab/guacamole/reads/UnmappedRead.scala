package org.hammerlab.guacamole.reads

import org.hammerlab.genomics.bases.Bases
import org.hammerlab.guacamole.readsets.SampleId

/**
 * An unmapped read. See the [[Read]] trait for field descriptions.
 *
 */
case class UnmappedRead(
    name: String,
    sequence: Bases,
    baseQualities: IndexedSeq[Byte],
    isDuplicate: Boolean,
    sampleId: SampleId,
    failedVendorQualityChecks: Boolean,
    isPaired: Boolean) extends Read {

  assert(baseQualities.length == sequence.length)

  override val isMapped = false
  override def asMappedRead = None
}

