package org.hammerlab.guacamole.filters.genotype

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.variants.{AlleleEvidence, CalledAllele}

/**
 * Filter to remove genotypes where the number of reads to support the alternate allele is low
 */
object MinimumAlternateReadDepthFilter {

  /**
   *
   * Apply the filter to an RDD of genotypes
   *
   * @param genotypes RDD of genotypes to filter
   * @param minAlternateReadDepth minimum number of reads with alternate allele at locus for this genotype
   * @param debug if true, compute the count of genotypes after filtering
   * @return Genotypes with read depth >= minAlternateReadDepth
   */
  def apply(genotypes: RDD[CalledAllele],
            minAlternateReadDepth: Int,
            debug: Boolean = false): RDD[CalledAllele] = {
    val filteredGenotypes = genotypes.filter(gt => hasMinimumAlternateReadDepth(gt.evidence, minAlternateReadDepth))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }

  def hasMinimumAlternateReadDepth(alleleEvidence: AlleleEvidence,
                                   minAlternateReadDepth: Int): Boolean = {
    alleleEvidence.alleleReadDepth >= minAlternateReadDepth
  }
}
