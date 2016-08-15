package org.hammerlab.guacamole.filters.somatic

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.variants.CalledSomaticAllele

object SomaticAlternateReadDepthFilter {

  def hasMinimumAlternateReadDepth(somaticGenotype: CalledSomaticAllele,
                                   minAlternateReadDepth: Int): Boolean = {
    somaticGenotype.tumorVariantEvidence.alleleReadDepth >= minAlternateReadDepth
  }

  /**
   *
   *  Apply the filter to an RDD of genotypes
   *
   * @param genotypes RDD of genotypes to filter
   * @param minAlternateReadDepth minimum number of reads with alternate allele at locus for this genotype in the tumor sample
   * @param debug if true, compute the count of genotypes after filtering
   * @return  Genotypes with tumor alternate read depth >= minAlternateReadDepth
   */
  def apply(genotypes: RDD[CalledSomaticAllele],
            minAlternateReadDepth: Int,
            debug: Boolean = false): RDD[CalledSomaticAllele] = {
    val filteredGenotypes = genotypes.filter(hasMinimumAlternateReadDepth(_, minAlternateReadDepth))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}
