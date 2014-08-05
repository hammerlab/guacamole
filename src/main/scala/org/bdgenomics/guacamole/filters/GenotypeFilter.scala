package org.bdgenomics.guacamole.filters

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.ADAMGenotype
import org.bdgenomics.guacamole.Common.Arguments.Base
import org.kohsuke.args4j.Option
import org.bdgenomics.guacamole.Common

/**
 * Filter to remove genotypes where the number of reads at the locus is low
 */
object MinimumLikelihoodFilter {

  def hasMinimumLikelihood(genotype: ADAMGenotype,
                           minLikelihood: Int,
                           includeNull: Boolean = true): Boolean = {
    if (genotype.getGenotypeQuality != null) {
      genotype.getGenotypeQuality >= minLikelihood
    } else {
      includeNull
    }
  }

  /**
   *
   *  Apply the filter to an RDD of genotypes
   *
   * @param genotypes RDD of genotypes to filter
   * @param minLikelihood minimum quality score for this genotype
   * @param includeNull include the genotype if the required fields are null
   * @param debug if true, compute the count of genotypes after filtering
   * @return Genotypes with quality >= minLikelihood
   */
  def apply(genotypes: RDD[ADAMGenotype],
            minLikelihood: Int,
            debug: Boolean = false,
            includeNull: Boolean = true): RDD[ADAMGenotype] = {
    val filteredGenotypes = genotypes.filter(hasMinimumLikelihood(_, minLikelihood, includeNull))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

/**
 * Filter to remove genotypes where the number of reads at the locus is too low or too high
 */
object ReadDepthFilter {

  def withinReadDepthRange(genotype: ADAMGenotype,
                           minReadDepth: Int,
                           maxReadDepth: Int,
                           includeNull: Boolean = true): Boolean = {
    if (genotype.readDepth != null) {
      genotype.readDepth >= minReadDepth && genotype.readDepth < maxReadDepth
    } else {
      includeNull
    }
  }

  /**
   *
   *  Apply the filter to an RDD of genotypes
   *
   * @param genotypes RDD of genotypes to filter
   * @param minReadDepth minimum number of reads at locus for this genotype
   * @param maxReadDepth maximum number of reads at locus for this genotype
   * @param includeNull include the genotype if the required fields are null
   * @param debug if true, compute the count of genotypes after filtering
   * @return Genotypes with read depth >= minReadDepth and < maxReadDepth
   */
  def apply(genotypes: RDD[ADAMGenotype],
            minReadDepth: Int,
            maxReadDepth: Int,
            debug: Boolean = false,
            includeNull: Boolean = true): RDD[ADAMGenotype] = {
    val filteredGenotypes = genotypes.filter(withinReadDepthRange(_, minReadDepth, maxReadDepth, includeNull))
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
   * @param includeNull include the genotype if the required fields are null
   * @param debug if true, compute the count of genotypes after filtering
   * @return Genotypes with read depth >= minAlternateReadDepth
   */
  def apply(genotypes: RDD[ADAMGenotype],
            minAlternateReadDepth: Int,
            debug: Boolean = false,
            includeNull: Boolean = true): RDD[ADAMGenotype] = {
    val filteredGenotypes = genotypes.filter(hasMinimumAlternateReadDepth(_, minAlternateReadDepth, includeNull))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }

  def hasMinimumAlternateReadDepth(genotype: ADAMGenotype,
                                   minAlternateReadDepth: Int,
                                   includeNull: Boolean = true): Boolean = {
    if (genotype.alternateReadDepth != null) {
      genotype.alternateReadDepth >= minAlternateReadDepth
    } else {
      includeNull
    }
  }
}

object GenotypeFilter {

  def printFilterProgress(filteredGenotypes: RDD[ADAMGenotype]) = {
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

  def apply(genotypes: RDD[ADAMGenotype], args: GenotypeFilterArguments): RDD[ADAMGenotype] = {
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

  def apply(genotypes: Seq[ADAMGenotype],
            minReadDepth: Int,
            minAlternateReadDepth: Int,
            minLikelihood: Int,
            maxReadDepth: Int,
            alleleBalance: Int): Seq[ADAMGenotype] = {
    var filteredGenotypes = genotypes

    if (minReadDepth > 0) {
      filteredGenotypes = filteredGenotypes.filter(ReadDepthFilter.withinReadDepthRange(_, minReadDepth, maxReadDepth))
    }

    if (minAlternateReadDepth > 0) {
      filteredGenotypes = filteredGenotypes.filter(MinimumAlternateReadDepthFilter.hasMinimumAlternateReadDepth(_, minAlternateReadDepth))
    }

    filteredGenotypes
  }

}