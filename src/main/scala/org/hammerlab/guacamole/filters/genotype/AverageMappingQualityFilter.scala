package org.hammerlab.guacamole.filters.genotype

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.variants.CalledAllele

/**
 * Filter to remove genotypes when the mean mapping quality of the reads is low
 */
object AverageMappingQualityFilter {

  def hasMinimumAverageMappingQuality(genotype: CalledAllele,
                                      minAverageMappingQuality: Int): Boolean = {

    genotype.evidence.meanMappingQuality > minAverageMappingQuality
  }

  /**
   *
   * @param genotypes RDD of genotypes to filter
   * @param minAverageMappingQuality
   * @param debug if true, compute the count of genotypes after filtering
   * @return  Genotypes with reads where mean mapping quality > minAverageMappingQuality
   */
  def apply(genotypes: RDD[CalledAllele],
            minAverageMappingQuality: Int,
            debug: Boolean = false): RDD[CalledAllele] = {
    val filteredGenotypes = genotypes.filter(hasMinimumAverageMappingQuality(_, minAverageMappingQuality))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}
