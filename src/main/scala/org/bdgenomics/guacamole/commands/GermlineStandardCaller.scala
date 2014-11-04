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

package org.bdgenomics.guacamole.callers

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.guacamole.variants.{ AlleleEvidence, Genotype, AlleleConversions, CalledAllele }
import org.bdgenomics.guacamole._
import org.bdgenomics.guacamole.Common.Arguments._
import Concordance.ConcordanceArgs
import org.bdgenomics.guacamole.filters.GenotypeFilter.GenotypeFilterArguments
import org.bdgenomics.guacamole.filters.PileupFilter.PileupFilterArguments
import org.bdgenomics.guacamole.filters.{ GenotypeFilter, QualityAlignedReadsFilter }
import org.bdgenomics.guacamole.pileup.Pileup
import org.bdgenomics.guacamole.reads.Read
import org.kohsuke.args4j.Option

/**
 * Simple Bayesian variant caller implementation that uses the base and read quality score
 */
object GermlineStandardCaller extends Command with Serializable with Logging {
  override val name = "germline-standard"
  override val description = "call variants using a simple quality-based probability"

  private class Arguments extends Base
      with Output with Reads with ConcordanceArgs with GenotypeFilterArguments with PileupFilterArguments with DistributedUtil.Arguments {

    @Option(name = "-emit-ref", usage = "Output homozygous reference calls.")
    var emitRef: Boolean = false
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(appName = Some(name))

    val readSet = Common.loadReadsFromArguments(args, sc, Read.InputFilters(mapped = true, nonDuplicate = true))
    readSet.mappedReads.persist()
    Common.progress(
      "Loaded %,d mapped non-duplicate reads into %,d partitions.".format(readSet.mappedReads.count, readSet.mappedReads.partitions.length))

    val loci = Common.loci(args, readSet)
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, readSet.mappedReads)

    val minAlignmentQuality = args.minAlignmentQuality

    val genotypes: RDD[CalledAllele] = DistributedUtil.pileupFlatMap[CalledAllele](
      readSet.mappedReads,
      lociPartitions,
      skipEmpty = true, // skip empty pileups
      pileup => callVariantsAtLocus(pileup, minAlignmentQuality).iterator)
    readSet.mappedReads.unpersist()

    val filteredGenotypes = GenotypeFilter(genotypes, args).flatMap(AlleleConversions.calledAlleleToADAMGenotype)
    Common.writeVariantsFromArguments(args, filteredGenotypes)
    if (args.truthGenotypesFile != "")
      Concordance.printGenotypeConcordance(args, filteredGenotypes, sc)

    DelayedMessages.default.print()
  }

  /**
   * Computes the genotype and probability at a given locus
   *
   * @param pileup Collection of pileup elements at align to the locus
   * @param minAlignmentQuality minimum alignment quality for reads to consider (default: 0)
   * @param emitRef Also return all reference genotypes (default: false)
   *
   * @return Sequence of possible called genotypes for all samples
   */
  def callVariantsAtLocus(
    pileup: Pileup,
    minAlignmentQuality: Int = 0,
    emitRef: Boolean = false): Seq[CalledAllele] = {

    // For now, we skip loci that have no reads mapped. We may instead want to emit NoCall in this case.
    if (pileup.elements.isEmpty)
      return Seq.empty

    pileup.bySample.toSeq.flatMap({
      case (sampleName, samplePileup) =>
        val filteredPileupElements = QualityAlignedReadsFilter(samplePileup.elements, minAlignmentQuality)
        val genotypeLikelihoods = Pileup(samplePileup.locus, filteredPileupElements).computeLogLikelihoods()
        val mostLikelyGenotype = genotypeLikelihoods.maxBy(_._2)

        def buildVariants(genotype: Genotype, probability: Double): Seq[CalledAllele] = {
          genotype.getNonReferenceAlleles.map(allele => {
            CalledAllele(
              sampleName,
              samplePileup.referenceName,
              samplePileup.locus,
              allele,
              AlleleEvidence(
                probability,
                allele,
                samplePileup
              )
            )
          })
        }
        buildVariants(mostLikelyGenotype._1, mostLikelyGenotype._2)

    })
  }
}
