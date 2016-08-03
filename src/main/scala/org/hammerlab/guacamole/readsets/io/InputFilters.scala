package org.hammerlab.guacamole.readsets.io

import org.hammerlab.guacamole.loci.set.LociParser

/**
 * Filtering reads while they are loaded can be an important optimization.
 *
 * These fields are commonly used filters. For boolean fields, setting a field to true will result in filtering on
 * that field. The result is the intersection of the filters (i.e. reads must satisfy ALL filters).
 *
 * @param overlapsLoci if not None, include only mapped reads that overlap the given loci
 * @param nonDuplicate include only reads that do not have the duplicate bit set
 * @param passedVendorQualityChecks include only reads that do not have the failedVendorQualityChecks bit set
 * @param isPaired include only reads are paired-end reads
 */
case class InputFilters(overlapsLoci: Option[LociParser],
                        nonDuplicate: Boolean,
                        passedVendorQualityChecks: Boolean,
                        isPaired: Boolean)

object InputFilters {
  val empty = InputFilters()

  /**
   * See InputFilters for full documentation.
   *
   * @param mapped include only mapped reads. Convenience argument that is equivalent to specifying all sites in
   *               overlapsLoci.
   */
  def apply(mapped: Boolean = false,
            overlapsLoci: LociParser = null,
            nonDuplicate: Boolean = false,
            passedVendorQualityChecks: Boolean = false,
            isPaired: Boolean = false): InputFilters = {
    new InputFilters(
      overlapsLoci =
        if (overlapsLoci == null && mapped)
          Some(LociParser.all)
        else
          Option(overlapsLoci),
      nonDuplicate,
      passedVendorQualityChecks,
      isPaired
    )
  }
}

