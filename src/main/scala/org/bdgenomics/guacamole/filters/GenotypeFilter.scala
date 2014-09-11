package org.bdgenomics.guacamole.filters

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.Genotype
import org.bdgenomics.guacamole.Common.Arguments.Base
import org.kohsuke.args4j.Option
import org.bdgenomics.guacamole.Common

/**
 * Filter to remove genotypes where the number of reads at the locus is low
 */
object MinimumLikelihoodFilter {

  def hasMinimumLikelihood(genotype: Genotype,
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
  def apply(genotypes: RDD[Genotype],
            minLikelihood: Int,
            debug: Boolean = false,
            includeNull: Boolean = true): RDD[Genotype] = {
    val filteredGenotypes = genotypes.filter(hasMinimumLikelihood(_, minLikelihood, includeNull))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

/**
 * Filter to remove genotypes where the number of reads at the locus is too low or too high
 */
object ReadDepthFilter {

  def withinReadDepthRange(genotype: Genotype,
                           minReadDepth: Int,
                           maxReadDepth: Int,
                           includeNull: Boolean = true): Boolean = {
    if (genotype.getReadDepth != null) {
      genotype.getReadDepth >= minReadDepth && genotype.getReadDepth < maxReadDepth
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
  def apply(genotypes: RDD[Genotype],
            minReadDepth: Int,
            maxReadDepth: Int,
            debug: Boolean = false,
            includeNull: Boolean = true): RDD[Genotype] = {
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
  def apply(genotypes: RDD[Genotype],
            minAlternateReadDepth: Int,
            debug: Boolean = false,
            includeNull: Boolean = true): RDD[Genotype] = {
    val filteredGenotypes = genotypes.filter(hasMinimumAlternateReadDepth(_, minAlternateReadDepth, includeNull))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }

  def hasMinimumAlternateReadDepth(genotype: Genotype,
                                   minAlternateReadDepth: Int,
                                   includeNull: Boolean = true): Boolean = {
    if (genotype.getAlternateReadDepth != null) {
      genotype.getAlternateReadDepth >= minAlternateReadDepth
    } else {
      includeNull
    }
  }
}

object GenotypeFilter {

  def printFilterProgress(filteredGenotypes: RDD[Genotype]) = {
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

  def apply(genotypes: RDD[Genotype], args: GenotypeFilterArguments): RDD[Genotype] = {
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

  def apply(genotypes: Seq[Genotype],
            minReadDepth: Int,
            minAlternateReadDepth: Int,
            minLikelihood: Int,
            maxReadDepth: Int): Seq[Genotype] = {

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
