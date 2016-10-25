package org.hammerlab.guacamole.readsets.io

import org.hammerlab.guacamole.loci.parsing.ParsedLoci

/**
 * Filtering reads while they are loaded can be an important optimization.
 *
 * These fields are commonly used filters. For boolean fields, setting a field to true will result in filtering on
 * that field. The result is the intersection of the filters (i.e. reads must satisfy ALL filters).
 *
 * @param overlapsLociOpt if set, include only mapped reads that overlap the given loci
 * @param nonDuplicate include only reads that do not have the duplicate bit set
 * @param passedVendorQualityChecks include only reads that do not have the failedVendorQualityChecks bit set
 * @param isPaired include only reads are paired-end reads
 * @param minAlignmentQuality Minimum Phred-scaled alignment score for a read
 */
case class InputFilters(overlapsLociOpt: Option[ParsedLoci],
                        nonDuplicate: Boolean,
                        passedVendorQualityChecks: Boolean,
                        isPaired: Boolean,
                        minAlignmentQuality: Int,
                        maxSplitSizeOpt: Option[Int]
                       ) {
  def loci = overlapsLociOpt.getOrElse(ParsedLoci.all)
}

object InputFilters {
  val empty =
    InputFilters(
      overlapsLociOpt = None,
      nonDuplicate = false,
      passedVendorQualityChecks = false,
      isPaired = false,
      minAlignmentQuality = 0,
      maxSplitSizeOpt = None
    )
}
