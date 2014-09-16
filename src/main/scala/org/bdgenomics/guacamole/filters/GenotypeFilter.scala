package org.bdgenomics.guacamole.filters

import org.apache.spark.rdd.RDD
import org.bdgenomics.guacamole.Common
import org.bdgenomics.guacamole.Common.Arguments.Base
import org.bdgenomics.guacamole.variants.{ CalledGenotype, GenotypeEvidence }
import org.kohsuke.args4j.Option

/**
 * Filter to remove genotypes where the likelihood is low
 */
object MinimumLikelihoodFilter {

  def hasMinimumLikelihood(genotypeEvidence: GenotypeEvidence,
                           minLikelihood: Int): Boolean = {
    genotypeEvidence.phredScaledLikelihood >= minLikelihood
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
  def apply(genotypes: RDD[CalledGenotype],
            minLikelihood: Int,
            debug: Boolean = false): RDD[CalledGenotype] = {
    val filteredGenotypes = genotypes.filter(gt => hasMinimumLikelihood(gt.evidence, minLikelihood))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

/**
 * Filter to remove genotypes where the number of reads at the locus is too low or too high
 */
object ReadDepthFilter {

  def withinReadDepthRange(genotypeEvidence: GenotypeEvidence,
                           minReadDepth: Int,
                           maxReadDepth: Int): Boolean = {
    genotypeEvidence.readDepth >= minReadDepth && genotypeEvidence.readDepth < maxReadDepth
  }

  /**
   *
   *  Apply the filter to an RDD of genotypes
   *
   * @param genotypes RDD of genotypes to filter
   * @param minReadDepth minimum number of reads at locus for this genotype
   * @param maxReadDepth maximum number of reads at locus for this genotype
   * @param debug if true, compute the count of genotypes after filtering
   * @return Genotypes with read depth >= minReadDepth and < maxReadDepth
   */
  def apply(genotypes: RDD[CalledGenotype],
            minReadDepth: Int,
            maxReadDepth: Int,
            debug: Boolean = false): RDD[CalledGenotype] = {
    val filteredGenotypes = genotypes.filter(gt => withinReadDepthRange(gt.evidence, minReadDepth, maxReadDepth))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

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
  def apply(genotypes: RDD[CalledGenotype],
            minAlternateReadDepth: Int,
            debug: Boolean = false): RDD[CalledGenotype] = {
    val filteredGenotypes = genotypes.filter(gt => hasMinimumAlternateReadDepth(gt.evidence, minAlternateReadDepth))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }

  def hasMinimumAlternateReadDepth(genotypeEvidence: GenotypeEvidence,
                                   minAlternateReadDepth: Int): Boolean = {
    genotypeEvidence.alternateReadDepth >= minAlternateReadDepth
  }
}

object GenotypeFilter {

  def printFilterProgress(filteredGenotypes: RDD[CalledGenotype]) = {
    filteredGenotypes.persist()
    Common.progress("Filtered genotypes down to %d genotypes".format(filteredGenotypes.count()))
  }

  trait GenotypeFilterArguments extends Base {

    @Option(name = "-minReadDepth", usage = "Minimum number of reads for a genotype call")
    var minReadDepth: Int = 0

    @Option(name = "-maxReadDepth", usage = "Maximum number of reads for a genotype call")
    var maxReadDepth: Int = Int.MaxValue

    @Option(name = "-minAlternateReadDepth", usage = "Minimum number of reads with alternate allele for a genotype call")
    var minAlternateReadDepth: Int = 0

    @Option(name = "-debug-genotype-filters", usage = "Print count of genotypes after each filtering step")
    var debugGenotypeFilters = false

    @Option(name = "-minLikelihood", usage = "Minimum Phred-scaled likelihood. Default: 0 (off)")
    var minLikelihood: Int = 0

  }

  def apply(genotypes: RDD[CalledGenotype], args: GenotypeFilterArguments): RDD[CalledGenotype] = {
    var filteredGenotypes = genotypes

    filteredGenotypes = ReadDepthFilter(filteredGenotypes, args.minReadDepth, args.maxReadDepth, args.debugGenotypeFilters)

    if (args.minAlternateReadDepth > 0) {
      filteredGenotypes = MinimumAlternateReadDepthFilter(filteredGenotypes, args.minAlternateReadDepth, args.debugGenotypeFilters)
    }

    if (args.minLikelihood > 0) {
      filteredGenotypes = MinimumLikelihoodFilter(filteredGenotypes, args.minLikelihood, args.debugGenotypeFilters)
    }

    filteredGenotypes
  }

  def apply(genotypes: Seq[CalledGenotype],
            minReadDepth: Int,
            minAlternateReadDepth: Int,
            minLikelihood: Int,
            maxReadDepth: Int): Seq[CalledGenotype] = {
    var filteredGenotypes = genotypes

    if (minReadDepth > 0) {
      filteredGenotypes = filteredGenotypes.filter(gt => ReadDepthFilter.withinReadDepthRange(gt.evidence, minReadDepth, maxReadDepth))
    }

    if (minAlternateReadDepth > 0) {
      filteredGenotypes = filteredGenotypes.filter(gt => MinimumAlternateReadDepthFilter.hasMinimumAlternateReadDepth(gt.evidence, minAlternateReadDepth))
    }

    filteredGenotypes
  }

}
