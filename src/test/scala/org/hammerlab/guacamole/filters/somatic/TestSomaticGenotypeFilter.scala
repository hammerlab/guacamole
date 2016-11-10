package org.hammerlab.guacamole.filters.somatic

import org.hammerlab.guacamole.variants.CalledSomaticAllele

object TestSomaticGenotypeFilter {
  /**
   * Filter a sequence of Somatic Genotypes
   *  Utility function for testing
   */
  def apply(genotypes: Seq[CalledSomaticAllele],
            minTumorReadDepth: Int,
            maxTumorReadDepth: Int,
            minNormalReadDepth: Int,
            minTumorAlternateReadDepth: Int,
            minLogOdds: Int,
            minVAF: Int,
            minLikelihood: Int): Seq[CalledSomaticAllele] = {

    var filteredGenotypes = genotypes

    filteredGenotypes =
      filteredGenotypes.filter(SomaticReadDepthFilter.withinReadDepthRange(_, minTumorReadDepth, maxTumorReadDepth, minNormalReadDepth))

    filteredGenotypes =
      filteredGenotypes.filter(SomaticVAFFilter.hasMinimumVAF(_, minVAF))

    if (minLikelihood > 0)
      filteredGenotypes =
        filteredGenotypes.filter(SomaticMinimumLikelihoodFilter.hasMinimumLikelihood(_, minLikelihood))

    if (minTumorAlternateReadDepth > 0)
      filteredGenotypes =
        filteredGenotypes.filter(SomaticAlternateReadDepthFilter.hasMinimumAlternateReadDepth(_, minTumorAlternateReadDepth))

    filteredGenotypes
  }
}
