package org.bdgenomics.guacamole.filters

import org.apache.spark.rdd.RDD
import org.bdgenomics.guacamole.Common.Arguments.Base
import org.bdgenomics.guacamole._
import org.bdgenomics.guacamole.variants.{ CalledGenotype, GenotypeEvidence, CalledSomaticGenotype }
import org.kohsuke.args4j.Option

/**
 * Filter to remove genotypes where the somatic likelihood is low
 */
object SomaticMinimumLikelihoodFilter {

  def hasMinimumLikelihood(genotype: CalledSomaticGenotype,
                           minLikelihood: Int): Boolean = {
    genotype.phredScaledSomaticLikelihood >= minLikelihood
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
  def apply(genotypes: RDD[CalledSomaticGenotype],
            minLikelihood: Int,
            debug: Boolean = false): RDD[CalledSomaticGenotype] = {
    val filteredGenotypes = genotypes.filter(gt => hasMinimumLikelihood(gt, minLikelihood))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

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
    somaticGenotype.tumorEvidence.alleleReadDepth >= minAlternateReadDepth
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

object SomaticVAFFilter {

  def hasMinimumVAF(somaticGenotype: CalledSomaticGenotype,
                    minVAF: Int): Boolean = {

    somaticGenotype.tumorEvidence.variantAlleleFrequency * 100.0 > minVAF
  }

  /**
   *
   * @param genotypes RDD of genotypes to filter
   * @param minVAF minimum variant allele frequency
   * @param debug if true, compute the count of genotypes after filtering
   * @return  Genotypes with variant allele frequency >= minVAF
   */
  def apply(genotypes: RDD[CalledSomaticGenotype],
            minVAF: Int,
            debug: Boolean = false): RDD[CalledSomaticGenotype] = {
    val filteredGenotypes = genotypes.filter(hasMinimumVAF(_, minVAF))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

object SomaticLogOddsFilter {

  def hasMinimumLOD(somaticGenotype: CalledSomaticGenotype,
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
  def apply(genotypes: RDD[CalledSomaticGenotype],
            minLogOdds: Int,
            debug: Boolean = false): RDD[CalledSomaticGenotype] = {
    val filteredGenotypes = genotypes.filter(hasMinimumLOD(_, minLogOdds))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

object SomaticAverageMappingQualityFilter {

  def hasMinimumAverageMappingQuality(somaticGenotype: CalledSomaticGenotype,
                                      minAverageMappingQuality: Int): Boolean = {

    somaticGenotype.tumorEvidence.averageMappingQuality > minAverageMappingQuality &&
      somaticGenotype.normalEvidence.averageMappingQuality > minAverageMappingQuality
  }

  /**
   *
   * @param genotypes RDD of genotypes to filter
   * @param minAverageMappingQuality
   * @param debug if true, compute the count of genotypes after filtering
   * @return  Genotypes with variant allele frequency >= minVAF
   */
  def apply(genotypes: RDD[CalledSomaticGenotype],
            minAverageMappingQuality: Int,
            debug: Boolean = false): RDD[CalledSomaticGenotype] = {
    val filteredGenotypes = genotypes.filter(hasMinimumAverageMappingQuality(_, minAverageMappingQuality))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

object SomaticAverageBaseQualityFilter {

  def hasMinimumAverageBaseQuality(somaticGenotype: CalledSomaticGenotype,
                                   minAverageBaseQuality: Int): Boolean = {

    somaticGenotype.tumorEvidence.averageBaseQuality > minAverageBaseQuality &&
      somaticGenotype.normalEvidence.averageBaseQuality > minAverageBaseQuality
  }

  /**
   *
   * @param genotypes RDD of genotypes to filter
   * @param minAverageBaseQuality
   * @param debug if true, compute the count of genotypes after filtering
   * @return  Genotypes with variant allele frequency >= minVAF
   */
  def apply(genotypes: RDD[CalledSomaticGenotype],
            minAverageBaseQuality: Int,
            debug: Boolean = false): RDD[CalledSomaticGenotype] = {
    val filteredGenotypes = genotypes.filter(hasMinimumAverageBaseQuality(_, minAverageBaseQuality))
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

    @Option(name = "-minLikelihood", usage = "Minimum likelihood (Phred-scaled)")
    var minLikelihood: Int = 0

    @Option(name = "-minVAF", usage = "Minimum variant allele frequency")
    var minVAF: Int = 0

    @Option(name = "-minLOD", metaVar = "X", usage = "Make a call if the log odds of variant is greater than this value (Phred-scaled)")
    var minLOD: Int = 0

    @Option(name = "-minAverageMappingQuality", metaVar = "X", usage = "Make a call average mapping quality of reads is greater than this value")
    var minAverageMappingQuality: Int = 0

    @Option(name = "-minAverageBaseQuality", metaVar = "X", usage = "Make a call average base quality of bases in the pileup is greater than this value")
    var minAverageBaseQuality: Int = 0

    @Option(name = "-minTumorReadDepth", usage = "Minimum number of reads in tumor sample for a genotype call")
    var minTumorReadDepth: Int = 0

    @Option(name = "-minNormalReadDepth", usage = "Minimum number of reads in normal sample for a genotype call")
    var minNormalReadDepth: Int = 0

    @Option(name = "-maxTumorReadDepth", usage = "Maximum number of reads in tumor sample for a genotype call")
    var maxTumorReadDepth: Int = Int.MaxValue

    @Option(name = "-minTumorAlternateReadDepth", usage = "Minimum number of reads with alternate allele for a genotype call")
    var minTumorAlternateReadDepth: Int = 0

    @Option(name = "-debug-genotype-filters", usage = "Print count of genotypes after each filtering step")
    var debugGenotypeFilters = false

  }

  /**
   * Filter an RDD of Somatic Genotypes with all applicable filters
   */
  def apply(genotypes: RDD[CalledSomaticGenotype], args: SomaticGenotypeFilterArguments): RDD[CalledSomaticGenotype] = {
    var filteredGenotypes = genotypes

    filteredGenotypes = SomaticReadDepthFilter(filteredGenotypes, args.minTumorReadDepth, args.maxTumorReadDepth, args.minNormalReadDepth, args.debugGenotypeFilters)

    if (args.minTumorAlternateReadDepth > 0) {
      filteredGenotypes = SomaticAlternateReadDepthFilter(filteredGenotypes, args.minTumorAlternateReadDepth, args.debugGenotypeFilters)
    }

    filteredGenotypes = SomaticLogOddsFilter(filteredGenotypes, args.minLOD, args.debugGenotypeFilters)

    filteredGenotypes = SomaticMinimumLikelihoodFilter(filteredGenotypes, args.minLikelihood, args.debugGenotypeFilters)

    filteredGenotypes = SomaticVAFFilter(filteredGenotypes, args.minVAF, args.debugGenotypeFilters)

    filteredGenotypes = SomaticAverageMappingQualityFilter(filteredGenotypes, args.minAverageMappingQuality, args.debugGenotypeFilters)

    filteredGenotypes = SomaticAverageBaseQualityFilter(filteredGenotypes, args.minAverageBaseQuality, args.debugGenotypeFilters)

    filteredGenotypes
  }

  /**
   * Filter a sequence of Somatic Genotypes
   *  Utility function for testing
   */
  def apply(genotypes: Seq[CalledSomaticGenotype],
            minTumorReadDepth: Int,
            maxTumorReadDepth: Int,
            minNormalReadDepth: Int,
            minTumorAlternateReadDepth: Int,
            minLogOdds: Int,
            minVAF: Int,
            minLikelihood: Int): Seq[CalledSomaticGenotype] = {

    var filteredGenotypes = genotypes

    filteredGenotypes = filteredGenotypes.filter(SomaticReadDepthFilter.withinReadDepthRange(_, minTumorReadDepth, maxTumorReadDepth, minNormalReadDepth))

    filteredGenotypes = filteredGenotypes.filter(SomaticVAFFilter.hasMinimumVAF(_, minVAF))

    filteredGenotypes = filteredGenotypes.filter(SomaticMinimumLikelihoodFilter.hasMinimumLikelihood(_, minLikelihood))

    if (minTumorAlternateReadDepth > 0) {
      filteredGenotypes = filteredGenotypes.filter(SomaticAlternateReadDepthFilter.hasMinimumAlternateReadDepth(_, minTumorAlternateReadDepth))
    }

    filteredGenotypes
  }

}
