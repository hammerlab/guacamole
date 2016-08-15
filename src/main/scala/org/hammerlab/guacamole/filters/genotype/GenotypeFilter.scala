package org.hammerlab.guacamole.filters.genotype

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.logging.DebugLogArgs
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.variants.CalledAllele
import org.kohsuke.args4j.{Option => Args4jOption}

object GenotypeFilter {

  def printFilterProgress(filteredGenotypes: RDD[CalledAllele]) = {
    filteredGenotypes.persist()
    progress(s"Filtered genotypes down to ${filteredGenotypes.count} genotypes")
  }

  trait GenotypeFilterArguments extends DebugLogArgs {

    @Args4jOption(name = "--min-read-depth", usage = "Minimum number of reads for a genotype call")
    var minReadDepth: Int = 0

    @Args4jOption(name = "--max-read-depth", usage = "Maximum number of reads for a genotype call")
    var maxReadDepth: Int = Int.MaxValue

    @Args4jOption(name = "--min-alternate-read-depth", usage = "Minimum number of reads with alternate allele for a genotype call")
    var minAlternateReadDepth: Int = 0

    @Args4jOption(name = "--debug-genotype-filters", usage = "Print count of genotypes after each filtering step")
    var debugGenotypeFilters = false

    @Args4jOption(name = "--min-likelihood", usage = "Minimum Phred-scaled likelihood. Default: 0 (off)")
    var minLikelihood: Int = 0

    @Args4jOption(name = "--min-average-mapping-quality", metaVar = "X", usage = "Removes any variants where the average mapping quality of reads is less than this value")
    var minAverageMappingQuality: Int = 0

    @Args4jOption(name = "--min-average-base-quality", metaVar = "X", usage = "Removes any variants where the average base quality of bases in the pileup is less than this value")
    var minAverageBaseQuality: Int = 0

  }

  def apply(genotypes: RDD[CalledAllele], args: GenotypeFilterArguments): RDD[CalledAllele] = {
    var filteredGenotypes = genotypes

    filteredGenotypes = ReadDepthFilter(filteredGenotypes, args.minReadDepth, args.maxReadDepth, args.debugGenotypeFilters)

    if (args.minAlternateReadDepth > 0) {
      filteredGenotypes = MinimumAlternateReadDepthFilter(filteredGenotypes, args.minAlternateReadDepth, args.debugGenotypeFilters)
    }

    if (args.minLikelihood > 0) {
      filteredGenotypes = MinimumLikelihoodFilter(filteredGenotypes, args.minLikelihood, args.debugGenotypeFilters)
    }

    filteredGenotypes = AverageMappingQualityFilter(filteredGenotypes, args.minAverageMappingQuality, args.debugGenotypeFilters)

    filteredGenotypes = AverageBaseQualityFilter(filteredGenotypes, args.minAverageBaseQuality, args.debugGenotypeFilters)

    filteredGenotypes
  }

  def apply(genotypes: Seq[CalledAllele],
            minReadDepth: Int,
            minAlternateReadDepth: Int,
            minLikelihood: Int,
            maxReadDepth: Int): Seq[CalledAllele] = {
    var filteredGenotypes = genotypes

    if (minReadDepth > 0) {
      filteredGenotypes = filteredGenotypes.filter(gt => ReadDepthFilter.withinReadDepthRange(gt.evidence, minReadDepth, maxReadDepth))
    }

    if (minAlternateReadDepth > 0) {
      filteredGenotypes = filteredGenotypes.filter(gt => MinimumAlternateReadDepthFilter.hasMinimumAlternateReadDepth(gt.evidence, minAlternateReadDepth))
    }

    if (minAlternateReadDepth > 0) {
      filteredGenotypes = filteredGenotypes.filter(gt => MinimumAlternateReadDepthFilter.hasMinimumAlternateReadDepth(gt.evidence, minAlternateReadDepth))
    }

    filteredGenotypes
  }

}
