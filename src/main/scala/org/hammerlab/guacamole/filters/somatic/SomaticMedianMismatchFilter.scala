package org.hammerlab.guacamole.filters.somatic

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.variants.CalledSomaticAllele

object SomaticMedianMismatchFilter {

  def hasMaximumMedianMismatch(somaticGenotype: CalledSomaticAllele,
                               maximumMedianMismatches: Int): Boolean = {

    somaticGenotype.tumorVariantEvidence.medianMismatchesPerRead <= maximumMedianMismatches
  }

  /**
   *
   * @param genotypes RDD of genotypes to filter
   * @param maximumMedianMismatches Maximum median number of mismatches on a read
   * @param debug if true, compute the count of genotypes after filtering
   * @return  Genotypes with median mismatches on reads <= maximumMedianMismatches
   */
  def apply(genotypes: RDD[CalledSomaticAllele],
            maximumMedianMismatches: Int,
            debug: Boolean = false): RDD[CalledSomaticAllele] = {
    val filteredGenotypes = genotypes.filter(hasMaximumMedianMismatch(_, maximumMedianMismatches))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}
