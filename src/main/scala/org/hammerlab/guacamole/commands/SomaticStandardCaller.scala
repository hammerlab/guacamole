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
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.DatabaseVariantAnnotation
import org.hammerlab.guacamole.Common.Arguments.SomaticCallerArgs
import org.hammerlab.guacamole.filters.PileupFilter.PileupFilterArguments
import org.hammerlab.guacamole.filters.SomaticGenotypeFilter.SomaticGenotypeFilterArguments
import org.hammerlab.guacamole.filters.{PileupFilter, SomaticAlternateReadDepthFilter, SomaticGenotypeFilter, SomaticReadDepthFilter}
import org.hammerlab.guacamole.likelihood.Likelihood
import org.hammerlab.guacamole.logging.DelayedMessages
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.Read
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.variants.{Allele, AlleleConversions, AlleleEvidence, CalledSomaticAllele}
import org.hammerlab.guacamole.{Common, DistributedUtil, SparkCommand}
import org.kohsuke.args4j.{Option => Args4jOption}

/**
 * Simple subtraction based somatic variant caller
 *
 * This takes two variant callers, calls variants on tumor and normal independently,
 * and outputs the variants in the tumor sample BUT NOT the normal sample.
 *
 * This assumes that both read sets only contain a single sample, otherwise we should compare
 * on a sample identifier when joining the genotypes
 *
 */
object SomaticStandard {

  protected class Arguments extends SomaticCallerArgs with PileupFilterArguments with SomaticGenotypeFilterArguments {

    @Args4jOption(name = "--odds", usage = "Minimum log odds threshold for possible variant candidates")
    var oddsThreshold: Int = 20

    @Args4jOption(name = "--dbsnp-vcf", required = false, usage = "VCF file to identify DBSNP variants")
    var dbSnpVcf: String = ""

    @Args4jOption(name = "--reference-fasta", required = true, usage = "Local path to a reference FASTA file")
    var referenceFastaPath: String = ""

  }

  object Caller extends SparkCommand[Arguments] {
    override val name = "somatic-standard"
    override val description = "call somatic variants using independent callers on tumor and normal"

    override def run(args: Arguments, sc: SparkContext): Unit = {
      Common.validateArguments(args)
      val loci = Common.lociFromArguments(args)
      val filters = Read.InputFilters(
        overlapsLoci = Some(loci),
        nonDuplicate = true,
        passedVendorQualityChecks = true)

      val reference = ReferenceBroadcast(args.referenceFastaPath, sc)

      val (tumorReads, normalReads) =
        Common.loadTumorNormalReadsFromArguments(
          args,
          sc,
          filters
        )

      assert(tumorReads.sequenceDictionary == normalReads.sequenceDictionary,
        "Tumor and normal samples have different sequence dictionaries. Tumor dictionary: %s.\nNormal dictionary: %s."
          .format(tumorReads.sequenceDictionary, normalReads.sequenceDictionary))

      val filterMultiAllelic = args.filterMultiAllelic
      val minAlignmentQuality = args.minAlignmentQuality
      val maxReadDepth = args.maxTumorReadDepth

      val oddsThreshold = args.oddsThreshold

      val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(
        args,
        loci.result(normalReads.contigLengths),
        tumorReads.mappedReads,
        normalReads.mappedReads
      )

      var potentialGenotypes: RDD[CalledSomaticAllele] =
        DistributedUtil.pileupFlatMapTwoRDDs[CalledSomaticAllele](
          tumorReads.mappedReads,
          normalReads.mappedReads,
          lociPartitions,
          skipEmpty = true, // skip empty pileups
          function = (pileupTumor, pileupNormal) =>
            findPotentialVariantAtLocus(
              pileupTumor,
              pileupNormal,
              oddsThreshold,
              minAlignmentQuality,
              filterMultiAllelic,
              maxReadDepth
            ).iterator,
          reference = reference
        )

      potentialGenotypes.persist()
      progress("Computed %,d potential genotypes".format(potentialGenotypes.count))

      // Filter potential genotypes to min read values
      potentialGenotypes =
        SomaticReadDepthFilter(
          potentialGenotypes,
          args.minTumorReadDepth,
          args.maxTumorReadDepth,
          args.minNormalReadDepth
        )

      potentialGenotypes =
        SomaticAlternateReadDepthFilter(
          potentialGenotypes,
          args.minTumorAlternateReadDepth
        )

      if (args.dbSnpVcf != "") {
        val adamContext = new ADAMContext(sc)
        val dbSnpVariants = adamContext.loadVariantAnnotations(args.dbSnpVcf)
        potentialGenotypes = potentialGenotypes
          .keyBy(_.adamVariant)
          .leftOuterJoin(dbSnpVariants.keyBy(_.getVariant))
          .map(_._2).map({
            case (calledAllele: CalledSomaticAllele, dbSnpVariant: Option[DatabaseVariantAnnotation]) =>
              calledAllele.copy(rsID = dbSnpVariant.map(_.getDbSnpId))
          })
      }

      val filteredGenotypes: RDD[CalledSomaticAllele] = SomaticGenotypeFilter(potentialGenotypes, args)
      progress("Computed %,d genotypes after basic filtering".format(filteredGenotypes.count))

      Common.writeVariantsFromArguments(
        args,
        filteredGenotypes.flatMap(AlleleConversions.calledSomaticAlleleToADAMGenotype)
      )

      DelayedMessages.default.print()
    }

    def findPotentialVariantAtLocus(tumorPileup: Pileup,
                                    normalPileup: Pileup,
                                    oddsThreshold: Int,
                                    minAlignmentQuality: Int = 1,
                                    filterMultiAllelic: Boolean = false,
                                    maxReadDepth: Int = Int.MaxValue): Seq[CalledSomaticAllele] = {

      val filteredNormalPileup = PileupFilter(
        normalPileup,
        filterMultiAllelic,
        minAlignmentQuality,
        minEdgeDistance = 0
      )

      val filteredTumorPileup = PileupFilter(
        tumorPileup,
        filterMultiAllelic,
        minAlignmentQuality,
        minEdgeDistance = 0
      )

      // For now, we skip loci that have no reads mapped. We may instead want to emit NoCall in this case.
      if (filteredTumorPileup.elements.isEmpty
        || filteredNormalPileup.elements.isEmpty
        || filteredTumorPileup.depth > maxReadDepth // skip abnormally deep pileups
        || filteredNormalPileup.depth > maxReadDepth
        || filteredTumorPileup.referenceDepth == filteredTumorPileup.depth // skip computation if no alternate reads
        )
        return Seq.empty

      /**
       * Find the most likely genotype in the tumor sample
       * This is either the reference genotype or an heterozygous genotype with some alternate base
       */
      val genotypesAndLikelihoods = Likelihood.likelihoodsOfAllPossibleGenotypesFromPileup(
        filteredTumorPileup,
        Likelihood.probabilityCorrectIncludingAlignment,
        normalize = true)
      if (genotypesAndLikelihoods.isEmpty)
        return Seq.empty

      val (mostLikelyTumorGenotype, mostLikelyTumorGenotypeLikelihood) = genotypesAndLikelihoods.maxBy(_._2)

      // The following lazy vals are only evaluated if mostLikelyTumorGenotype.hasVariantAllele
      lazy val normalLikelihoods =
        Likelihood.likelihoodsOfAllPossibleGenotypesFromPileup(
          filteredNormalPileup,
          Likelihood.probabilityCorrectIgnoringAlignment,
          normalize = true).toMap
      lazy val normalVariantGenotypes = normalLikelihoods.filter(_._1.hasVariantAllele)

      // NOTE(ryan): for now, compare non-reference alleles found in tumor to the sum of all likelihoods of variant
      // genotypes in the normal sample.
      // TODO(ryan): in the future, we may want to pay closer attention to the likelihood of the most likely tumor
      // genotype in the normal sample.
      lazy val normalVariantsTotalLikelihood = normalVariantGenotypes.map(_._2).sum
      lazy val somaticOdds = mostLikelyTumorGenotypeLikelihood / normalVariantsTotalLikelihood

      if (mostLikelyTumorGenotype.hasVariantAllele
        && somaticOdds * 100 >= oddsThreshold) {
        for {
          // NOTE(ryan): currently only look at the first non-ref allele in the most likely tumor genotype.
          // removeCorrelatedGenotypes depends on there only being one variant per locus.
          // TODO(ryan): if we want to handle the possibility of two non-reference alleles at a locus, iterate over all
          // non-reference alleles here and rework downstream assumptions accordingly.
          allele <- mostLikelyTumorGenotype.getNonReferenceAlleles.find(_.altBases.nonEmpty).toSeq
          tumorVariantEvidence = AlleleEvidence(mostLikelyTumorGenotypeLikelihood, allele, filteredTumorPileup)
          normalReferenceEvidence = AlleleEvidence(1 - normalVariantsTotalLikelihood, Allele(allele.refBases, allele.refBases), filteredNormalPileup)
        } yield {
          CalledSomaticAllele(
            tumorPileup.sampleName,
            tumorPileup.referenceName,
            tumorPileup.locus,
            allele,
            math.log(somaticOdds),
            tumorVariantEvidence,
            normalReferenceEvidence
          )
        }
      } else {
        Seq()
      }

    }
  }
}
