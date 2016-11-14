package org.hammerlab.guacamole.readsets.io

import org.hammerlab.guacamole.loci.parsing.ParsedLoci

object TestInputConfig {
  def mapped(nonDuplicate: Boolean = false,
             passedVendorQualityChecks: Boolean = false,
             isPaired: Boolean = false,
             minAlignmentQuality: Int = 0): InputConfig =
    new InputConfig(
      overlapsLociOpt = Some(ParsedLoci.all),
      nonDuplicate,
      passedVendorQualityChecks,
      isPaired,
      if (minAlignmentQuality > 0) Some(minAlignmentQuality) else None,
      maxSplitSizeOpt = None
    )

  def apply(overlapsLoci: ParsedLoci,
            nonDuplicate: Boolean = false,
            passedVendorQualityChecks: Boolean = false,
            isPaired: Boolean = false,
            minAlignmentQuality: Int = 0): InputConfig =
    new InputConfig(
      overlapsLociOpt = Some(overlapsLoci),
      nonDuplicate,
      passedVendorQualityChecks,
      isPaired,
      if (minAlignmentQuality > 0) Some(minAlignmentQuality) else None,
      maxSplitSizeOpt = None
    )
}
