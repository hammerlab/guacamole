package org.hammerlab.guacamole.readsets.io

import org.hammerlab.guacamole.loci.parsing.ParsedLoci

object TestInputFilters {
  def mapped(nonDuplicate: Boolean = false,
             passedVendorQualityChecks: Boolean = false,
             isPaired: Boolean = false,
             minAlignmentQuality: Int = 0): InputFilters =
    new InputFilters(
      overlapsLociOpt = Some(ParsedLoci.all),
      nonDuplicate,
      passedVendorQualityChecks,
      isPaired,
      if (minAlignmentQuality > 0) Some(minAlignmentQuality) else None
    )

  def apply(overlapsLoci: ParsedLoci,
            nonDuplicate: Boolean = false,
            passedVendorQualityChecks: Boolean = false,
            isPaired: Boolean = false,
            minAlignmentQuality: Int = 0): InputFilters =
    new InputFilters(
      overlapsLociOpt = Some(overlapsLoci),
      nonDuplicate,
      passedVendorQualityChecks,
      isPaired,
      if (minAlignmentQuality > 0) Some(minAlignmentQuality) else None
    )
}
