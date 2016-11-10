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

package org.hammerlab.guacamole.filters.somatic

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.util.IntOptionHandler
import org.hammerlab.guacamole.variants.CalledSomaticAllele
import org.kohsuke.args4j.{Option => Args4jOption}

object SomaticGenotypeFilter {

  def printFilterProgress(filteredGenotypes: RDD[CalledSomaticAllele]) = {
    filteredGenotypes.persist()
    progress(s"Filtered genotypes down to ${filteredGenotypes.count} genotypes")
  }

  trait SomaticGenotypeFilterArguments {

    @Args4jOption(
      name = "--min-likelihood",
      usage = "Minimum likelihood (Phred-scaled)",
      handler = classOf[IntOptionHandler]
    )
    var minLikelihoodOpt: Option[Int] = None

    @Args4jOption(name = "--min-vaf", usage = "Minimum variant allele frequency")
    var minVAF: Int = 0

    @Args4jOption(name = "--min-average-mapping-quality", metaVar = "X", usage = "Removes any variants where the average mapping quality of reads is less than this value")
    var minAverageMappingQuality: Int = 20

    @Args4jOption(name = "--min-average-base-quality", metaVar = "X", usage = "Removes any variants where the average base quality of bases in the pileup is less than this value")
    var minAverageBaseQuality: Int = 20

    @Args4jOption(name = "--min-tumor-read-depth", usage = "Minimum number of reads in tumor sample for a genotype call")
    var minTumorReadDepth: Int = 5

    @Args4jOption(name = "--min-normal-read-depth", usage = "Minimum number of reads in normal sample for a genotype call")
    var minNormalReadDepth: Int = 5

    @Args4jOption(name = "--max-tumor-read-depth", usage = "Maximum number of reads in tumor sample for a genotype call")
    var maxTumorReadDepth: Int = Int.MaxValue

    @Args4jOption(
      name = "--min-tumor-alternate-read-depth",
      usage = "Minimum number of reads with alternate allele for a genotype call",
      handler = classOf[IntOptionHandler]
    )
    var minTumorAlternateReadDepthOpt: Option[Int] = None

    @Args4jOption(
      name = "--max-median-mismatches",
      usage = "Maximum median number of mismatches on a read",
      handler = classOf[IntOptionHandler]
    )
    var maximumMedianMismatchesOpt: Option[Int] = None

    @Args4jOption(name = "--debug-genotype-filters", usage = "Print count of genotypes after each filtering step")
    var debugGenotypeFilters = false
  }

  /**
   * Filter an RDD of Somatic Genotypes with all applicable filters
   */
  def apply(genotypes: RDD[CalledSomaticAllele], args: SomaticGenotypeFilterArguments): RDD[CalledSomaticAllele] = {
    var filteredGenotypes = genotypes

    filteredGenotypes =
      SomaticReadDepthFilter(
        filteredGenotypes,
        args.minTumorReadDepth,
        args.maxTumorReadDepth,
        args.minNormalReadDepth,
        args.debugGenotypeFilters
      )

    args.minTumorAlternateReadDepthOpt.foreach(
      minTumorAlternateReadDepthOpt =>
        filteredGenotypes =
          SomaticAlternateReadDepthFilter(
            filteredGenotypes,
            minTumorAlternateReadDepthOpt,
            args.debugGenotypeFilters
          )
    )

    args.minLikelihoodOpt.foreach(
      minLikelihood =>
        filteredGenotypes =
          SomaticMinimumLikelihoodFilter(
            filteredGenotypes,
            minLikelihood,
            args.debugGenotypeFilters
          )
    )

    filteredGenotypes =
      SomaticVAFFilter(filteredGenotypes, args.minVAF, args.debugGenotypeFilters)

    filteredGenotypes =
      SomaticAverageMappingQualityFilter(filteredGenotypes, args.minAverageMappingQuality, args.debugGenotypeFilters)

    filteredGenotypes =
      SomaticAverageBaseQualityFilter(filteredGenotypes, args.minAverageBaseQuality, args.debugGenotypeFilters)

    args.maximumMedianMismatchesOpt.foreach(
      maximumMedianMismatches =>
        filteredGenotypes =
          SomaticMedianMismatchFilter(filteredGenotypes, maximumMedianMismatches, args.debugGenotypeFilters)
    )

    filteredGenotypes
  }
}
