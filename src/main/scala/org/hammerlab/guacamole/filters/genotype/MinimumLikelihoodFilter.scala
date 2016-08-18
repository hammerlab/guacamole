package org.hammerlab.guacamole.filters.genotype

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.variants.{AlleleEvidence, CalledAllele}

/**
 * Filter to remove genotypes where the likelihood is low
 */
object MinimumLikelihoodFilter {

  def hasMinimumLikelihood(alleleEvidence: AlleleEvidence,
                           minLikelihood: Int): Boolean = {
    alleleEvidence.phredScaledLikelihood >= minLikelihood
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
  def apply(genotypes: RDD[CalledAllele],
            minLikelihood: Int,
            debug: Boolean = false): RDD[CalledAllele] = {
    val filteredGenotypes = genotypes.filter(gt => hasMinimumLikelihood(gt.evidence, minLikelihood))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}
