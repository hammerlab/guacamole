package org.bdgenomics.guacamole.reads

/**
 * An unmapped read. See the [[Read]] trait for field descriptions.
 *
 */
case class UnmappedRead(
    token: Int,
    sequence: Seq[Byte],
    baseQualities: Seq[Byte],
    isDuplicate: Boolean,
    sampleName: String,
    failedVendorQualityChecks: Boolean,
    isPositiveStrand: Boolean,
    matePropertiesOpt: Option[MateProperties]) extends Read {

  assert(baseQualities.length == sequence.length)
}

