package org.hammerlab.guacamole.filters.somatic

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.variants.CalledSomaticAllele

/**
 * Filter to remove genotypes where the somatic likelihood is low
 */
object SomaticMinimumLikelihoodFilter {

  def hasMinimumLikelihood(genotype: CalledSomaticAllele,
                           minLikelihood: Int): Boolean = {
    genotype.phredScaledSomaticLikelihood >= minLikelihood
  }

  /**
   *
   *  Apply the filter to an RDD of genotypes
   *
   * @param genotypes RDD of genotypes to filter
   * @param minLikelihood minimum quality score for this genotype (Phred-scaled)
   * @param debug if true, compute the count of genotypes after filtering
   * @return Genotypes with quality >= minLikelihood
   */
  def apply(genotypes: RDD[CalledSomaticAllele],
            minLikelihood: Int,
            debug: Boolean = false): RDD[CalledSomaticAllele] = {
    val filteredGenotypes = genotypes.filter(gt => hasMinimumLikelihood(gt, minLikelihood))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}
