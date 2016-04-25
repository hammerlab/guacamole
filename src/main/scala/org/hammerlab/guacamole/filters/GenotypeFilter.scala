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
import org.hammerlab.guacamole.logging.DebugLogArgs
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.variants.{AlleleEvidence, CalledAllele}
import org.kohsuke.args4j.{Option => Args4jOption}

/**
 * Filter to remove genotypes where the likelihood is low
 */
object MinimumLikelihoodFilter {

  def hasMinimumLikelihood(alleleEvidence: AlleleEvidence,
                           minLikelihood: Int): Boolean = {
    alleleEvidence.phredScaledLikelihood >= minLikelihood
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
  def apply(genotypes: RDD[CalledAllele],
            minLikelihood: Int,
            debug: Boolean = false): RDD[CalledAllele] = {
    val filteredGenotypes = genotypes.filter(gt => hasMinimumLikelihood(gt.evidence, minLikelihood))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

/**
 * Filter to remove genotypes where the number of reads at the locus is too low or too high
 */
object ReadDepthFilter {

  def withinReadDepthRange(alleleEvidence: AlleleEvidence,
                           minReadDepth: Int,
                           maxReadDepth: Int): Boolean = {
    alleleEvidence.readDepth >= minReadDepth && alleleEvidence.readDepth < maxReadDepth
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
  def apply(genotypes: RDD[CalledAllele],
            minReadDepth: Int,
            maxReadDepth: Int,
            debug: Boolean = false): RDD[CalledAllele] = {
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
  def apply(genotypes: RDD[CalledAllele],
            minAlternateReadDepth: Int,
            debug: Boolean = false): RDD[CalledAllele] = {
    val filteredGenotypes = genotypes.filter(gt => hasMinimumAlternateReadDepth(gt.evidence, minAlternateReadDepth))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }

  def hasMinimumAlternateReadDepth(alleleEvidence: AlleleEvidence,
                                   minAlternateReadDepth: Int): Boolean = {
    alleleEvidence.alleleReadDepth >= minAlternateReadDepth
  }
}

/**
 * Filter to remove genotypes when the mean mapping quality of the reads is low
 */
object AverageMappingQualityFilter {

  def hasMinimumAverageMappingQuality(genotype: CalledAllele,
                                      minAverageMappingQuality: Int): Boolean = {

    genotype.evidence.meanMappingQuality > minAverageMappingQuality
  }

  /**
   *
   * @param genotypes RDD of genotypes to filter
   * @param minAverageMappingQuality
   * @param debug if true, compute the count of genotypes after filtering
   * @return  Genotypes with reads where mean mapping quality > minAverageMappingQuality
   */
  def apply(genotypes: RDD[CalledAllele],
            minAverageMappingQuality: Int,
            debug: Boolean = false): RDD[CalledAllele] = {
    val filteredGenotypes = genotypes.filter(hasMinimumAverageMappingQuality(_, minAverageMappingQuality))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

/**
 * Filter to remove genotypes when the mean base quality of the bases is low
 */
object AverageBaseQualityFilter {

  def hasMinimumAverageBaseQuality(genotype: CalledAllele,
                                   minAverageBaseQuality: Int): Boolean = {
    genotype.evidence.meanBaseQuality > minAverageBaseQuality
  }

  /**
   *
   * @param genotypes RDD of genotypes to filter
   * @param minAverageBaseQuality
   * @param debug if true, compute the count of genotypes after filtering
   * @return  Genotypes with bases where mean base quality > minAverageBaseQuality
   */
  def apply(genotypes: RDD[CalledAllele],
            minAverageBaseQuality: Int,
            debug: Boolean = false): RDD[CalledAllele] = {
    val filteredGenotypes = genotypes.filter(hasMinimumAverageBaseQuality(_, minAverageBaseQuality))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

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
