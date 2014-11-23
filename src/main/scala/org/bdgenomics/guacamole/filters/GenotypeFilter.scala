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

package org.bdgenomics.guacamole.filters

import org.apache.spark.rdd.RDD
import org.bdgenomics.guacamole.Common
import org.bdgenomics.guacamole.Common.Arguments.Base
import org.bdgenomics.guacamole.variants.{ CalledAllele, AlleleEvidence }
import org.kohsuke.args4j.Option

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

object GenotypeFilter {

  def printFilterProgress(filteredGenotypes: RDD[CalledAllele]) = {
    filteredGenotypes.persist()
    Common.progress("Filtered genotypes down to %d genotypes".format(filteredGenotypes.count()))
  }

  trait GenotypeFilterArguments extends Base {

    @Option(name = "--min-read-depth", usage = "Minimum number of reads for a genotype call")
    var minReadDepth: Int = 0

    @Option(name = "--max-read-depth", usage = "Maximum number of reads for a genotype call")
    var maxReadDepth: Int = Int.MaxValue

    @Option(name = "--min-alternate-read-depth", usage = "Minimum number of reads with alternate allele for a genotype call")
    var minAlternateReadDepth: Int = 0

    @Option(name = "--debug-genotype-filters", usage = "Print count of genotypes after each filtering step")
    var debugGenotypeFilters = false

    @Option(name = "--min-likelihood", usage = "Minimum Phred-scaled likelihood. Default: 0 (off)")
    var minLikelihood: Int = 0

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

    filteredGenotypes
  }

}
