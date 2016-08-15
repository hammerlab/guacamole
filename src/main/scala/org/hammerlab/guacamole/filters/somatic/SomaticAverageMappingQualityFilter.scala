package org.hammerlab.guacamole.filters.somatic

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.variants.CalledSomaticAllele

object SomaticAverageMappingQualityFilter {

  def hasMinimumAverageMappingQuality(somaticGenotype: CalledSomaticAllele,
                                      minAverageMappingQuality: Int): Boolean = {

    somaticGenotype.tumorVariantEvidence.meanMappingQuality >= minAverageMappingQuality &&
      somaticGenotype.normalReferenceEvidence.meanMappingQuality >= minAverageMappingQuality
  }

  /**
   *
   * @param genotypes RDD of genotypes to filter
   * @param minAverageMappingQuality
   * @param debug if true, compute the count of genotypes after filtering
   * @return  Genotypes with mean average mapping quality >= minAverageMappingQuality
   */
  def apply(genotypes: RDD[CalledSomaticAllele],
            minAverageMappingQuality: Int,
            debug: Boolean = false): RDD[CalledSomaticAllele] = {
    val filteredGenotypes = genotypes.filter(hasMinimumAverageMappingQuality(_, minAverageMappingQuality))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}
