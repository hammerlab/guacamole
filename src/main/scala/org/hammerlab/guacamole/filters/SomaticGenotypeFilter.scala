/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hammerlab.guacamole.filters

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.Common.Arguments.Base
import org.hammerlab.guacamole._
import org.hammerlab.guacamole.variants.CalledSomaticAllele
import org.kohsuke.args4j.{ Option => Args4jOption }

/**
 * Filter to remove genotypes where the somatic likelihood is low
 */
object SomaticMinimumLikelihoodFilter {

  def hasMinimumLikelihood(genotype: CalledSomaticAllele,
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
  def apply(genotypes: RDD[CalledSomaticAllele],
            minLikelihood: Int,
            debug: Boolean = false): RDD[CalledSomaticAllele] = {
    val filteredGenotypes = genotypes.filter(gt => hasMinimumLikelihood(gt, minLikelihood))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

/**
 * Filter to remove genotypes where the number of reads at the locus is too low or too high
 */
object SomaticReadDepthFilter {

  def withinReadDepthRange(somaticGenotype: CalledSomaticAllele,
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
  def apply(genotypes: RDD[CalledSomaticAllele],
            minTumorReadDepth: Int,
            maxTumorReadDepth: Int,
            minNormalReadDepth: Int,
            debug: Boolean = false): RDD[CalledSomaticAllele] = {
    var filteredGenotypes = genotypes.filter(withinReadDepthRange(_, minTumorReadDepth, maxTumorReadDepth, minNormalReadDepth))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

object SomaticAlternateReadDepthFilter {

  def hasMinimumAlternateReadDepth(somaticGenotype: CalledSomaticAllele,
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
  def apply(genotypes: RDD[CalledSomaticAllele],
            minAlternateReadDepth: Int,
            debug: Boolean = false): RDD[CalledSomaticAllele] = {
    val filteredGenotypes = genotypes.filter(hasMinimumAlternateReadDepth(_, minAlternateReadDepth))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

object SomaticVAFFilter {

  def hasMinimumVAF(somaticGenotype: CalledSomaticAllele,
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
  def apply(genotypes: RDD[CalledSomaticAllele],
            minVAF: Int,
            debug: Boolean = false): RDD[CalledSomaticAllele] = {
    val filteredGenotypes = genotypes.filter(hasMinimumVAF(_, minVAF))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

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

object SomaticAverageMappingQualityFilter {

  def hasMinimumAverageMappingQuality(somaticGenotype: CalledSomaticAllele,
                                      minAverageMappingQuality: Int): Boolean = {

    somaticGenotype.tumorEvidence.meanMappingQuality >= minAverageMappingQuality &&
      somaticGenotype.normalEvidence.meanMappingQuality >= minAverageMappingQuality
  }

  /**
   *
   * @param genotypes RDD of genotypes to filter
   * @param minAverageMappingQuality
   * @param debug if true, compute the count of genotypes after filtering
   * @return  Genotypes with mean average mapping quality >= minAverageMappingQuality
   */
  def apply(genotypes: RDD[CalledSomaticAllele],
            minAverageMappingQuality: Int,
            debug: Boolean = false): RDD[CalledSomaticAllele] = {
    val filteredGenotypes = genotypes.filter(hasMinimumAverageMappingQuality(_, minAverageMappingQuality))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

object SomaticAverageBaseQualityFilter {

  def hasMinimumAverageBaseQuality(somaticGenotype: CalledSomaticAllele,
                                   minAverageBaseQuality: Int): Boolean = {

    somaticGenotype.tumorEvidence.meanMappingQuality >= minAverageBaseQuality &&
      somaticGenotype.normalEvidence.meanMappingQuality >= minAverageBaseQuality
  }

  /**
   *
   * @param genotypes RDD of genotypes to filter
   * @param minAverageBaseQuality
   * @param debug if true, compute the count of genotypes after filtering
   * @return  Genotypes with mean average base quality >= minAverageBaseQuality
   */
  def apply(genotypes: RDD[CalledSomaticAllele],
            minAverageBaseQuality: Int,
            debug: Boolean = false): RDD[CalledSomaticAllele] = {
    val filteredGenotypes = genotypes.filter(hasMinimumAverageBaseQuality(_, minAverageBaseQuality))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

object SomaticMedianMismatchFilter {

  def hasMaximumMedianMismatch(somaticGenotype: CalledSomaticAllele,
                               maximumMedianMismatches: Int): Boolean = {

    somaticGenotype.tumorEvidence.medianMismatchesPerRead <= maximumMedianMismatches
  }

  /**
   *
   * @param genotypes RDD of genotypes to filter
   * @param maximumMedianMismatches Maximum median number of mismatches on a read
   * @param debug if true, compute the count of genotypes after filtering
   * @return  Genotypes with median mismatches on reads <= maximumMedianMismatches
   */
  def apply(genotypes: RDD[CalledSomaticAllele],
            maximumMedianMismatches: Int,
            debug: Boolean = false): RDD[CalledSomaticAllele] = {
    val filteredGenotypes = genotypes.filter(hasMaximumMedianMismatch(_, maximumMedianMismatches))
    if (debug) SomaticGenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

object SomaticGenotypeFilter {

  def printFilterProgress(filteredGenotypes: RDD[CalledSomaticAllele]) = {
    filteredGenotypes.persist()
    Common.progress("Filtered genotypes down to %d genotypes".format(filteredGenotypes.count()))
  }

  trait SomaticGenotypeFilterArguments extends Base {

    @Args4jOption(name = "--min-likelihood", usage = "Minimum likelihood (Phred-scaled)")
    var minLikelihood: Int = 0

    @Args4jOption(name = "--min-vaf", usage = "Minimum variant allele frequency")
    var minVAF: Int = 0

    @Args4jOption(name = "--min-lod", metaVar = "X", usage = "Make a call if the log odds of variant is greater than this value (Phred-scaled)")
    var minLOD: Int = 0

    @Args4jOption(name = "--min-average-mapping-quality", metaVar = "X", usage = "Make a call average mapping quality of reads is greater than this value")
    var minAverageMappingQuality: Int = 0

    @Args4jOption(name = "--min-average-base-quality", metaVar = "X", usage = "Make a call average base quality of bases in the pileup is greater than this value")
    var minAverageBaseQuality: Int = 0

    @Args4jOption(name = "--min-tumor-read-depth", usage = "Minimum number of reads in tumor sample for a genotype call")
    var minTumorReadDepth: Int = 0

    @Args4jOption(name = "--min-normal-read-depth", usage = "Minimum number of reads in normal sample for a genotype call")
    var minNormalReadDepth: Int = 0

    @Args4jOption(name = "--max-tumor-read-depth", usage = "Maximum number of reads in tumor sample for a genotype call")
    var maxTumorReadDepth: Int = Int.MaxValue

    @Args4jOption(name = "--min-tumor-alternate-read-depth", usage = "Minimum number of reads with alternate allele for a genotype call")
    var minTumorAlternateReadDepth: Int = 0

    @Args4jOption(name = "--max-median-mismatches", usage = "Maximum median number of mismatches on a read")
    var maximumMedianMismatches: Int = Int.MaxValue

    @Args4jOption(name = "--debug-genotype-filters", usage = "Print count of genotypes after each filtering step")
    var debugGenotypeFilters = false

  }

  /**
   * Filter an RDD of Somatic Genotypes with all applicable filters
   */
  def apply(genotypes: RDD[CalledSomaticAllele], args: SomaticGenotypeFilterArguments): RDD[CalledSomaticAllele] = {
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

    filteredGenotypes = SomaticMedianMismatchFilter(filteredGenotypes, args.maximumMedianMismatches, args.debugGenotypeFilters)

    filteredGenotypes
  }

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

    filteredGenotypes = filteredGenotypes.filter(SomaticReadDepthFilter.withinReadDepthRange(_, minTumorReadDepth, maxTumorReadDepth, minNormalReadDepth))

    filteredGenotypes = filteredGenotypes.filter(SomaticVAFFilter.hasMinimumVAF(_, minVAF))

    filteredGenotypes = filteredGenotypes.filter(SomaticMinimumLikelihoodFilter.hasMinimumLikelihood(_, minLikelihood))

    if (minTumorAlternateReadDepth > 0) {
      filteredGenotypes = filteredGenotypes.filter(SomaticAlternateReadDepthFilter.hasMinimumAlternateReadDepth(_, minTumorAlternateReadDepth))
    }

    filteredGenotypes
  }

}
