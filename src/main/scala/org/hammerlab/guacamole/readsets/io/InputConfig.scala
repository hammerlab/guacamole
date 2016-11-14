package org.hammerlab.guacamole.readsets.io

import org.hammerlab.guacamole.loci.parsing.ParsedLoci

/**
 * Configuring how/which reads are loaded can be an important optimization.
 *
 * Most of these fields are commonly used filters. For boolean fields, setting a field to true will result in filtering
 * on that field. The result is the intersection of the filters (i.e. reads must satisfy ALL filters).
 *
 * @param overlapsLociOpt if set, include only mapped reads that overlap the given loci
 * @param nonDuplicate include only reads that do not have the duplicate bit set
 * @param passedVendorQualityChecks include only reads that do not have the failedVendorQualityChecks bit set
 * @param isPaired include only reads are paired-end reads
 * @param minAlignmentQualityOpt Minimum Phred-scaled alignment score for a read
 * @param maxSplitSizeOpt Maximum on-disk size to allow Hadoop splits to be; useful to set below the on-disk block-size
 *                        for BAMs where the reads compress extra-well, resulting in unwieldy numbers of reads per
 *                        fixed-disk-size partition.
 */
case class InputConfig(overlapsLociOpt: Option[ParsedLoci],
                       nonDuplicate: Boolean,
                       passedVendorQualityChecks: Boolean,
                       isPaired: Boolean,
                       minAlignmentQualityOpt: Option[Int],
                       maxSplitSizeOpt: Option[Long]
                       ) {
  def loci = overlapsLociOpt.getOrElse(ParsedLoci.all)
}

object InputConfig {
  val empty =
    InputConfig(
      overlapsLociOpt = None,
      nonDuplicate = false,
      passedVendorQualityChecks = false,
      isPaired = false,
      minAlignmentQualityOpt = None,
      maxSplitSizeOpt = None
    )
}
