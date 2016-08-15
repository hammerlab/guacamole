package org.hammerlab.guacamole.filters.genotype

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.variants.CalledAllele

/**
 * Filter to remove genotypes when the mean base quality of the bases is low
 */
object AverageBaseQualityFilter {

  def hasMinimumAverageBaseQuality(genotype: CalledAllele,
                                   minAverageBaseQuality: Int): Boolean =
    genotype.evidence.meanBaseQuality > minAverageBaseQuality

  /**
   *
   * @param genotypes RDD of genotypes to filter
   * @param minAverageBaseQuality
   * @param debug if true, compute the count of genotypes after filtering
   * @return  Genotypes with bases where mean base quality > minAverageBaseQuality
   */
  def apply(genotypes: RDD[CalledAllele],
            minAverageBaseQuality: Int,
            debug: Boolean = false): RDD[CalledAllele] = {
    val filteredGenotypes = genotypes.filter(hasMinimumAverageBaseQuality(_, minAverageBaseQuality))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}
