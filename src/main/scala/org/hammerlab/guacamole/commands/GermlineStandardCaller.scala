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

package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.Common.Arguments.GermlineCallerArgs
import org.hammerlab.guacamole.filters.GenotypeFilter.GenotypeFilterArguments
import org.hammerlab.guacamole.filters.PileupFilter.PileupFilterArguments
import org.hammerlab.guacamole.filters.{ GenotypeFilter, QualityAlignedReadsFilter }
import org.hammerlab.guacamole.likelihood.Likelihood
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.Read
import org.hammerlab.guacamole.variants.{ AlleleConversions, AlleleEvidence, CalledAllele }
import org.hammerlab.guacamole.{ Common, Concordance, DelayedMessages, DistributedUtil, SparkCommand }
import org.kohsuke.args4j.Option

/**
 * Simple Bayesian variant caller implementation that uses the base and read quality score
 */
object GermlineStandard {

  protected class Arguments extends GermlineCallerArgs with PileupFilterArguments with GenotypeFilterArguments {

    @Option(name = "--emit-ref", usage = "Output homozygous reference calls.")
    var emitRef: Boolean = false
  }

  object Caller extends SparkCommand[Arguments] {
    override val name = "germline-standard"
    override val description = "call variants using a simple quality-based probability"

    override def run(args: Arguments, sc: SparkContext): Unit = {
      Common.validateArguments(args)
      val readSet = Common.loadReadsFromArguments(
        args, sc, Read.InputFilters(mapped = true, nonDuplicate = true, hasMdTag = true))
      readSet.mappedReads.persist()
      Common.progress(
        "Loaded %,d mapped non-duplicate reads into %,d partitions.".format(readSet.mappedReads.count, readSet.mappedReads.partitions.length))

      val loci = Common.loci(args).result(readSet.contigLengths)
      val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, readSet.mappedReads)

      val minAlignmentQuality = args.minAlignmentQuality

      val genotypes: RDD[CalledAllele] = DistributedUtil.pileupFlatMap[CalledAllele](
        readSet.mappedReads,
        lociPartitions,
        skipEmpty = true, // skip empty pileups
        function = pileup => callVariantsAtLocus(pileup, minAlignmentQuality).iterator)
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
    def callVariantsAtLocus(pileup: Pileup,
                            minAlignmentQuality: Int = 0,
                            emitRef: Boolean = false): Seq[CalledAllele] = {

      // For now, we skip loci that have no reads mapped. We may instead want to emit NoCall in this case.
      if (pileup.elements.isEmpty)
        return Seq.empty

      pileup.bySample.toSeq.flatMap({
        case (sampleName, samplePileup) => {
          val filteredPileupElements = QualityAlignedReadsFilter(samplePileup.elements, minAlignmentQuality)
          if (filteredPileupElements.isEmpty) {
            // Similarly to above, we skip samples that have no reads after filtering.
            Seq.empty
          } else {
            val genotypeLikelihoods = Likelihood.likelihoodsOfAllPossibleGenotypesFromPileup(
              Pileup(samplePileup.referenceName, samplePileup.locus, samplePileup.referenceBase, filteredPileupElements),
              logSpace = true,
              normalize = true)
            val mostLikelyGenotypeAndProbability = genotypeLikelihoods.maxBy(_._2)

            val genotype = mostLikelyGenotypeAndProbability._1
            val probability = math.exp(mostLikelyGenotypeAndProbability._2)
            genotype.getNonReferenceAlleles.map(allele => {
              CalledAllele(
                sampleName,
                samplePileup.referenceName,
                samplePileup.locus,
                allele,
                AlleleEvidence(probability, allele, samplePileup))
            })
          }
        }
      })
    }
  }
}
