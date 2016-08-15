package org.hammerlab.guacamole.filters.somatic

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.variants.CalledSomaticAllele

object SomaticAverageBaseQualityFilter {

  def hasMinimumAverageBaseQuality(somaticGenotype: CalledSomaticAllele,
                                   minAverageBaseQuality: Int): Boolean = {

    somaticGenotype.tumorVariantEvidence.meanMappingQuality >= minAverageBaseQuality &&
      somaticGenotype.normalReferenceEvidence.meanMappingQuality >= minAverageBaseQuality
  }

  /**
   *
   * @param genotypes RDD of genotypes to filter
   * @param minAverageBaseQuality
   * @param debug if true, compute the count of genotypes after filtering
   * @return  Genotypes with mean average base quality >= minAverageBaseQuality
   */
  def apply(genotypes: RDD[CalledSomaticAllele],
            minAverageBaseQuality: Int,
            debug: Boolean = false): RDD[CalledSomaticAllele] = {
    val filteredGenotypes = genotypes.filter(hasMinimumAverageBaseQuality(_, minAverageBaseQuality))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}
