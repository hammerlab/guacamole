package org.hammerlab.guacamole.filters.somatic

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.filters.genotype.ReadDepthFilter
import org.hammerlab.guacamole.variants.CalledSomaticAllele

/**
 * Filter to remove genotypes where the number of reads at the locus is too low or too high
 */
object SomaticReadDepthFilter {

  def withinReadDepthRange(somaticGenotype: CalledSomaticAllele,
                           minTumorReadDepth: Int,
                           maxTumorReadDepth: Int,
                           minNormalReadDepth: Int): Boolean = {

    ReadDepthFilter.withinReadDepthRange(somaticGenotype.tumorVariantEvidence, minTumorReadDepth, maxTumorReadDepth) &&
      ReadDepthFilter.withinReadDepthRange(somaticGenotype.normalReferenceEvidence, minNormalReadDepth, Int.MaxValue)
  }

  /**
   *
   *  Apply the filter to an RDD of genotypes
   *
   * @param genotypes RDD of genotypes to filter
   * @param minTumorReadDepth minimum number of reads at locus for this genotype in the tumor sample
   * @param maxTumorReadDepth maximum number of reads at locus for this genotype in the tumor sample
   * @param minNormalReadDepth maximum number of reads at locus for this genotype in the normal sample
   * @param debug if true, compute the count of genotypes after filtering
   * @return Genotypes with read depth >= minReadDepth and < maxReadDepth
   */
  def apply(genotypes: RDD[CalledSomaticAllele],
            minTumorReadDepth: Int,
            maxTumorReadDepth: Int,
            minNormalReadDepth: Int,
            debug: Boolean = false): RDD[CalledSomaticAllele] = {
    val filteredGenotypes =
      genotypes.filter(
        withinReadDepthRange(_, minTumorReadDepth, maxTumorReadDepth, minNormalReadDepth)
      )

    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}
