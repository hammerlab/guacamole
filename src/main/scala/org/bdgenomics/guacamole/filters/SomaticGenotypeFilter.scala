package org.bdgenomics.guacamole.filters

import org.bdgenomics.guacamole._
import org.apache.spark.rdd.RDD
import org.bdgenomics.guacamole.Common.Arguments.Base
import org.kohsuke.args4j.Option

/**
 * Filter to remove genotypes where the number of reads at the locus is too low or too high
 */
object SomaticReadDepthFilter {

  def withinReadDepthRange(somaticGenotype: CalledSomaticGenotype,
                           minTumorReadDepth: Int,
                           maxTumorReadDepth: Int,
                           minNormalReadDepth: Int): Boolean = {

    ReadDepthFilter.withinReadDepthRange(somaticGenotype.tumorEvidence, minTumorReadDepth, maxTumorReadDepth) &&
      ReadDepthFilter.withinReadDepthRange(somaticGenotype.normalEvidence, minNormalReadDepth, Int.MaxValue)
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
  def apply(genotypes: RDD[CalledSomaticGenotype],
            minTumorReadDepth: Int,
            maxTumorReadDepth: Int,
            minNormalReadDepth: Int,
            debug: Boolean = false): RDD[CalledSomaticGenotype] = {
    var filteredGenotypes = genotypes.filter(withinReadDepthRange(_, minTumorReadDepth, maxTumorReadDepth, minNormalReadDepth))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

object SomaticAlternateReadDepthFilter {

  def hasMinimumAlternateReadDepth(somaticGenotype: CalledSomaticGenotype,
                                   minAlternateReadDepth: Int): Boolean = {
    somaticGenotype.tumorEvidence.alternateReadDepth >= minAlternateReadDepth
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
  def apply(genotypes: RDD[CalledSomaticGenotype],
            minAlternateReadDepth: Int,
            debug: Boolean = false): RDD[CalledSomaticGenotype] = {
    val filteredGenotypes = genotypes.filter(hasMinimumAlternateReadDepth(_, minAlternateReadDepth))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

object SomaticGenotypeFilter {

  def printFilterProgress(filteredGenotypes: RDD[CalledSomaticGenotype]) = {
    filteredGenotypes.persist()
    Common.progress("Filtered genotypes down to %d genotypes".format(filteredGenotypes.count()))
  }

  trait SomaticGenotypeFilterArguments extends Base {

    @Option(name = "-minTumorReadDepth", usage = "Minimum number of reads for a genotype call")
    var minTumorReadDepth: Int = 0

    @Option(name = "-minNormalReadDepth", usage = "Minimum number of reads for a genotype call")
    var minNormalReadDepth: Int = 0

    @Option(name = "-maxTumorReadDepth", usage = "Minimum number of reads for a genotype call")
    var maxTumorReadDepth: Int = Int.MaxValue

    @Option(name = "-minTumorAlternateReadDepth", usage = "Minimum number of reads with alternate allele for a genotype call")
    var minTumorAlternateReadDepth: Int = 0

    @Option(name = "-debug-genotype-filters", usage = "Print count of genotypes after each filtering step")
    var debugGenotypeFilters = false

  }

  def apply(genotypes: RDD[CalledSomaticGenotype], args: SomaticGenotypeFilterArguments): RDD[CalledSomaticGenotype] = {
    var filteredGenotypes = genotypes

    filteredGenotypes = SomaticReadDepthFilter(filteredGenotypes, args.minTumorReadDepth, args.maxTumorReadDepth, args.minNormalReadDepth, args.debugGenotypeFilters)

    if (args.minTumorAlternateReadDepth > 0) {
      filteredGenotypes = SomaticAlternateReadDepthFilter(filteredGenotypes, args.minTumorAlternateReadDepth)
    }

    filteredGenotypes
  }

  def apply(genotypes: Seq[CalledSomaticGenotype],
            minTumorReadDepth: Int,
            maxTumorReadDepth: Int,
            minNormalReadDepth: Int,
            minTumorAlternateReadDepth: Int): Seq[CalledSomaticGenotype] = {

    var filteredGenotypes = genotypes

    filteredGenotypes = filteredGenotypes.filter(SomaticReadDepthFilter.withinReadDepthRange(_, minTumorReadDepth, maxTumorReadDepth, minNormalReadDepth))

    if (minTumorAlternateReadDepth > 0) {
      filteredGenotypes = filteredGenotypes.filter(SomaticAlternateReadDepthFilter.hasMinimumAlternateReadDepth(_, minTumorAlternateReadDepth))
    }

    filteredGenotypes
  }

}