package org.bdgenomics.guacamole.reads

/**
 * An unmapped read. See the [[Read]] trait for field descriptions.
 *
 */
case class UnmappedRead(
    token: Int,
    sequence: Array[Byte],
    baseQualities: Array[Byte],
    isDuplicate: Boolean,
    sampleName: String,
    failedVendorQualityChecks: Boolean,
    isPositiveStrand: Boolean,
    matePropertiesOpt: Option[MateProperties]) extends Read {

  assert(baseQualities.length == sequence.length)

  final override val isMapped = false
}

