package org.hammerlab.guacamole.filters.somatic

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.variants.CalledSomaticAllele

object SomaticLogOddsFilter {

  def hasMinimumLOD(somaticGenotype: CalledSomaticAllele,
                    minLogOdds: Int): Boolean = {

    somaticGenotype.somaticLogOdds > minLogOdds
  }

  /**
   *
   * @param genotypes RDD of genotypes to filter
   * @param minLogOdds minimum log odd difference between tumor and normal genotypes
   * @param debug if true, compute the count of genotypes after filtering
   * @return  Genotypes with tumor genotype log odds >= minLOD
   */
  def apply(genotypes: RDD[CalledSomaticAllele],
            minLogOdds: Int,
            debug: Boolean = false): RDD[CalledSomaticAllele] = {
    val filteredGenotypes = genotypes.filter(hasMinimumLOD(_, minLogOdds))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}
