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

import breeze.linalg.min
import htsjdk.samtools.CigarOperator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.DatabaseVariantAnnotation
import org.hammerlab.guacamole.Common.Arguments.SomaticCallerArgs
import org.hammerlab.guacamole.likelihood._
import org.hammerlab.guacamole.pileup.{ Pileup, PileupElement }
import org.hammerlab.guacamole.reads.{ MDTagUtils, MappedRead, Read }
import org.hammerlab.guacamole.variants.{ CalledMutectSomaticAllele, _ }
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole._
import org.kohsuke.args4j.{ Option => Args4jOption }

import scala.annotation.tailrec

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
object SomaticMutectLike {

  protected class Arguments extends SomaticCallerArgs {

    @Args4jOption(name = "--min-tumor-log-odds", usage = "Minimum log odds for possible variant candidates in the tumor")
    var tumorLODThreshold: Double = DefaultMutectArgs.tumorLODThreshold

    @Args4jOption(name = "--min-normal-dbsnp-log-odds", usage = "Minimum log odds not heterozygous in the normal given dbSNP")
    var somDbSnpLODThreshold: Double = DefaultMutectArgs.somDbSnpLODThreshold

    @Args4jOption(name = "--min-normal-novel-log-odds", usage = "Minimum log odds not heterozygous in the normal given novel")
    var somNovelLODThreshold: Double = DefaultMutectArgs.somNovelLODThreshold

    @Args4jOption(name = "--minAlignmentQuality", usage = "Minimum alignment quality for pileup filtering")
    var minAlignmentQuality: Int = DefaultMutectArgs.minAlignmentQuality

    @Args4jOption(name = "--minBaseQuality", usage = "Minimum base quality for pileup filtering")
    var minBaseQuality: Int = DefaultMutectArgs.minBaseQuality

    @Args4jOption(name = "--maxGapEventsThresholdForPointMutations", usage = "Maximum number of nearby indels to call a SNP")
    var maxGapEventsThresholdForPointMutations: Int = DefaultMutectArgs.maxGapEventsThresholdForPointMutations

    @Args4jOption(name = "--minPassStringentFiltersTumor", usage = "Minimum fraction of reads passing the tumor stringent filters")
    var minPassStringentFiltersTumor: Double = DefaultMutectArgs.minPassStringentFiltersTumor

    @Args4jOption(name = "--maxMapq0Fraction", usage = "Maximum fraction of mapq 0 reads to allow a site")
    var maxMapq0Fraction: Double = DefaultMutectArgs.maxMapq0Fraction

    @Args4jOption(name = "--minPhredSupportingMutant", usage = "Minimum phred score of base supporting a mutation")
    var minPhredSupportingMutant: Int = DefaultMutectArgs.minPhredSupportingMutant

    @Args4jOption(name = "--indelNearnessThresholdForPointMutations", usage = "How close is too close for an indel to a point mutation")
    var indelNearnessThresholdForPointMutations: Int = DefaultMutectArgs.indelNearnessThresholdForPointMutations

    @Args4jOption(name = "--maxPhredSumMismatchingBases", usage = "Maximum sum of phred scores of mismatching bases in a tumor read to allow it")
    var maxPhredSumMismatchingBases: Int = DefaultMutectArgs.maxPhredSumMismatchingBases

    @Args4jOption(name = "--maxFractionBasesSoftClippedTumor", usage = "Maximum fraction of bases soft/hard clipped from a tumor read")
    var maxFractionBasesSoftClippedTumor: Double = DefaultMutectArgs.maxFractionBasesSoftClippedTumor

    @Args4jOption(name = "--maxNormalSupportingFracToTriggerQscoreCheck", usage = "Maximum fraction of alt alleles in normal to trigger qscore check")
    var maxNormalSupportingFracToTriggerQscoreCheck: Double = DefaultMutectArgs.maxNormalSupportingFracToTriggerQscoreCheck

    @Args4jOption(name = "--maxNormalQscoreSumSupportingMutant", usage = "Max qscore sum of mutant in normal sample if triggered by fraction")
    var maxNormalQscoreSumSupportingMutant: Int = DefaultMutectArgs.maxNormalQscoreSumSupportingMutant

    @Args4jOption(name = "--minMedianDistanceFromReadEnd", usage = "Minimal median distance of allele from ends of reads")
    var minMedianDistanceFromReadEnd: Int = DefaultMutectArgs.minMedianDistanceFromReadEnd

    @Args4jOption(name = "--minMedianAbsoluteDeviationOfAlleleInRead", usage = "Minimal absolute deviation of allele in reads")
    var minMedianAbsoluteDeviationOfAlleleInRead: Int = DefaultMutectArgs.minMedianAbsoluteDeviationOfAlleleInRead

    @Args4jOption(name = "--errorForPowerCalculations", usage = "Assumed error rate in power calculations")
    var errorForPowerCalculations: Double = DefaultMutectArgs.errorForPowerCalculations

    @Args4jOption(name = "--minThetaForPowerCalc", usage = "Assumed theta in power calculations")
    var minThetaForPowerCalc: Int = DefaultMutectArgs.minThetaForPowerCalc

    //    @Args4jOption(name = "--contamFrac", usage = "Fraction of contaminating reads in normal/tumor samples")
    //    var contamFrac: Double = DefaultMutectArgs.contamFrac.getOrElse(0.0)

    @Args4jOption(name = "--maxReadDepth", usage = "Maximum read depth to consider")
    var maxReadDepth: Int = DefaultMutectArgs.maxReadDepth

    @Args4jOption(name = "--dbsnp-vcf", required = false, usage = "VCF file to identify DBSNP variants")
    var dbSnpVcf: String = ""

    @Args4jOption(name = "--cosmic-vcf", required = false, usage = "VCF file to identify Cosmic variants")
    var cosmicVcf: String = ""

    @Args4jOption(name = "--noisy-muts-vcf", required = false, usage = "VCF file to filter noisy variants")
    var noiseVcf: String = ""

    @Args4jOption(name = "--reference-fasta", required = false, usage = "Local path to a reference FASTA file")
    var referenceFastaPath: String = null

  }

  object DefaultMutectArgs {
    val minAlignmentQuality: Int = 1
    val minBaseQuality: Int = 5
    val tumorLODThreshold: Double = 6.3
    val somDbSnpLODThreshold: Double = 5.5
    val somNovelLODThreshold: Double = 2.2
    val maxGapEventsThresholdForPointMutations: Int = 3
    val minPassStringentFiltersTumor: Double = 0.7
    val maxMapq0Fraction: Double = 0.5
    val minPhredSupportingMutant: Int = 20
    val indelNearnessThresholdForPointMutations: Int = 5
    val maxPhredSumMismatchingBases: Int = 100
    val maxFractionBasesSoftClippedTumor: Double = 0.3
    val maxNormalSupportingFracToTriggerQscoreCheck: Double = 0.015
    val maxNormalQscoreSumSupportingMutant: Int = 20
    val minMedianDistanceFromReadEnd: Int = 10
    val minMedianAbsoluteDeviationOfAlleleInRead: Int = 3
    val errorForPowerCalculations: Double = 0.001
    val minThetaForPowerCalc: Int = 20
    //val contamFrac: Option[Double] = None
    val maxReadDepth: Int = Int.MaxValue

  }

  object Caller extends SparkCommand[Arguments] {
    override val name = "somatic-mutect-like"
    override val description = "call somatic variants using the mutect method on tumor and normal"

    override def run(args: Arguments, sc: SparkContext): Unit = {

      Common.validateArguments(args)
      val loci = Common.lociFromArguments(args)

      val filters = Read.InputFilters(
        overlapsLoci = Some(loci),
        nonDuplicate = true,
        passedVendorQualityChecks = true,
        hasMdTag = true)

      val reference = Option(args.referenceFastaPath).map(ReferenceBroadcast(_, sc))

      val (tumorReads, normalReads) =
        Common.loadTumorNormalReadsFromArguments(
          args,
          sc,
          filters,
          requireMDTagsOnMappedReads = true,
          referenceGenome = reference
        )

      assert(tumorReads.sequenceDictionary == normalReads.sequenceDictionary,
        "Tumor and normal samples have different sequence dictionaries. Tumor dictionary: %s.\nNormal dictionary: %s."
          .format(tumorReads.sequenceDictionary, normalReads.sequenceDictionary))

      val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(
        args,
        loci.result(normalReads.contigLengths),
        tumorReads.mappedReads,
        normalReads.mappedReads
      )

      var potentialGenotypes: RDD[CalledMutectSomaticAllele] =
        DistributedUtil.pileupFlatMapTwoRDDs[CalledMutectSomaticAllele](
          tumorReads.mappedReads,
          normalReads.mappedReads,
          lociPartitions,
          skipEmpty = true, // skip empty pileups
          function = (pileupTumor, pileupNormal) =>
            findPotentialVariantAtLocus(
              rawTumorPileup = pileupTumor,
              rawNormalPileup = pileupNormal,
              minAlignmentQuality = args.minAlignmentQuality,
              minBaseQuality = args.minBaseQuality,
              tumorLODThreshold = args.tumorLODThreshold,
              maxPhredSumMismatchingBases = args.maxPhredSumMismatchingBases,
              indelNearnessThresholdForPointMutations = args.indelNearnessThresholdForPointMutations,
              maxFractionBasesSoftClippedTumor = args.maxFractionBasesSoftClippedTumor,
              errorForPowerCalculations = args.errorForPowerCalculations,
              minThetaForPowerCalc = args.minThetaForPowerCalc,
              //contamFrac = Some(args.contamFrac),
              maxReadDepth = args.maxReadDepth).iterator,
          referenceGenome = reference
        ).filter(c => mutectHeuristicFiltersPreDbLookup(call = c,
            minThetaForPowerCalc = args.minThetaForPowerCalc,
            maxGapEventsThresholdForPointMutations = args.maxGapEventsThresholdForPointMutations,
            minPassStringentFiltersTumor = args.minPassStringentFiltersTumor,
            maxMapq0Fraction = args.maxMapq0Fraction,
            minPhredSupportingMutant = args.minPhredSupportingMutant,
            maxNormalSupportingFracToTriggerQscoreCheck = args.maxNormalSupportingFracToTriggerQscoreCheck,
            maxNormalQscoreSumSupportingMutant = args.maxNormalQscoreSumSupportingMutant,
            minMedianDistanceFromReadEnd = args.minMedianDistanceFromReadEnd,
            minMedianAbsoluteDeviationOfAlleleInRead = args.minMedianAbsoluteDeviationOfAlleleInRead))

      potentialGenotypes.persist()
      Common.progress("Computed %,d potential genotypes".format(potentialGenotypes.count))

      if (args.dbSnpVcf != "") {
        val adamContext = new ADAMContext(sc)
        val dbSnpVariants = adamContext.loadVariantAnnotations(args.dbSnpVcf)
        potentialGenotypes = potentialGenotypes
          .keyBy(_.adamVariant)
          .leftOuterJoin(dbSnpVariants.keyBy(_.getVariant))
          .map(_._2).map({
            case (calledAllele: CalledMutectSomaticAllele, dbSnpVariant: Option[DatabaseVariantAnnotation]) =>
              calledAllele.copy(rsID = dbSnpVariant.map(_.getDbSnpId))
          })
      }

      if (args.cosmicVcf != "") {
        val adamContext = new ADAMContext(sc)
        val dbSnpVariants = adamContext.loadVariantAnnotations(args.cosmicVcf)
        potentialGenotypes = potentialGenotypes
          .keyBy(_.adamVariant)
          .leftOuterJoin(dbSnpVariants.keyBy(_.getVariant))
          .map(_._2).map({
            case (calledAllele: CalledMutectSomaticAllele, cosmicVariant: Option[DatabaseVariantAnnotation]) =>
              calledAllele.copy(cosOverlap = cosmicVariant.map(_ => true))
          })
      }

      if (args.noiseVcf != "") {
        val adamContext = new ADAMContext(sc)
        val dbSnpVariants = adamContext.loadVariantAnnotations(args.cosmicVcf)
        potentialGenotypes = potentialGenotypes
          .keyBy(_.adamVariant)
          .leftOuterJoin(dbSnpVariants.keyBy(_.getVariant))
          .map(_._2).map({
            case (calledAllele: CalledMutectSomaticAllele, noiseVariant: Option[DatabaseVariantAnnotation]) =>
              calledAllele.copy(noiseOverlap = noiseVariant.map(_ => true))
          })
      }

      val filteredGenotypes: RDD[CalledMutectSomaticAllele] = potentialGenotypes.filter(
        finalMutectDbSnpCosmicNoisyFilter(_,
          args.somDbSnpLODThreshold,
          args.somNovelLODThreshold))

      Common.progress("Computed %,d genotypes after basic filtering".format(filteredGenotypes.count))

      Common.writeVariantsFromArguments(
        args,
        filteredGenotypes.flatMap(AlleleConversions.calledSomaticAlleleToADAMGenotype)
      )

      DelayedMessages.default.print()
    }

    def finalMutectDbSnpCosmicNoisyFilter(somAllele: CalledMutectSomaticAllele, somDbSnpThreshold: Double, somNovelThreshold: Double): Boolean = {
      val recurrentMutSite = somAllele.cosOverlap.getOrElse(false)
      val noisyMutSite = somAllele.noiseOverlap.getOrElse(false)
      val dbSNPsite = somAllele.rsID.isDefined
      val passSomatic: Boolean = ((dbSNPsite && somAllele.mutectEvidence.normalNotHet >= somDbSnpThreshold) ||
        (!dbSNPsite && somAllele.mutectEvidence.normalNotHet >= somNovelThreshold))
      val passNoiseFilter: Boolean = (recurrentMutSite || !noisyMutSite)

      passSomatic && passNoiseFilter
    }

    def mapqBaseqFilteredPu(pu: Pileup, minBaseQuality: Int, minAlignmentQuality: Int): Pileup = {
      Pileup(pu.referenceName,
        pu.locus,
        pu.referenceBase,
        pu.elements.filter(pe =>
          pe.qualityScore >= minBaseQuality && pe.read.alignmentQuality >= minAlignmentQuality))
    }

    def heavilyFilteredPu(pu: Pileup, maxFractionBasesSoftClippedTumor: Double, maxPhredSumMismatchingBases: Int): Pileup = {
      Pileup(pu.referenceName,
        pu.locus,
        pu.referenceBase,
        pu.elements.filterNot(pe => {
          val clippedFilter = isReadHeavilyClipped(pe.read, maxFractionBasesSoftClippedTumor)
          val noisyFilter = pe.read.mdTagOpt.map(m =>
            MDTagUtils.getMismatchingQscoreSum(m, pe.read.baseQualities, pe.read.cigar) >= maxPhredSumMismatchingBases
          ).getOrElse(false)
          val mateRescueFilter = false //TODO determine if we want to do XT=M tag filtering, only relevant for BWA
          clippedFilter || noisyFilter || mateRescueFilter
        })
      )
    }

    def overlappingFragmentFilteredPileup(pu: Pileup): Pileup = {
      Pileup(pu.referenceName,
        pu.locus,
        pu.referenceBase,
        pu.elements.groupBy(pe => pe.read.name).map(nameSeq => {
          val pileups = nameSeq._2
          pileups.tail.fold(pileups.head)(
            (pu1: PileupElement, pu2: PileupElement) => if (pu1.qualityScore >= pu2.qualityScore) pu1 else pu2
          ) //chose the pileup element with the best quality score
        }).toSeq)
    }

    def isReadHeavilyClipped(read: MappedRead, maxFractionBasesSoftClippedTumor: Double): Boolean = {
      def isClipped(cigarOperator: CigarOperator): Boolean = Set(CigarOperator.SOFT_CLIP, CigarOperator.HARD_CLIP).contains(cigarOperator)

      val cigar = read.cigarElements
      val trimmedBeginning = if (isClipped(cigar.head.getOperator)) cigar.head.getLength else 0
      val trimmedEnd = if (cigar.length > 1 && isClipped(cigar.last.getOperator)) cigar.last.getLength else 0
      val readLen = read.cigarElements.filter(c => Set(CigarOperator.INSERTION, CigarOperator.MATCH_OR_MISMATCH).contains(c.getOperator)).map(_.getLength).sum
      (trimmedBeginning + trimmedEnd) / (readLen + trimmedBeginning + trimmedEnd) >= maxFractionBasesSoftClippedTumor
    }

    def distanceToNearestReadInsertionOrDeletion(pe: PileupElement, findInsertions: Boolean): Option[Int] = {
      def distanceToCigarElement(cigarOperator: CigarOperator): Option[Int] = {
        val distanceToThisElementBegin = pe.indexWithinCigarElement
        val distanceToThisElementEnd = pe.cigarElement.getLength - distanceToThisElementBegin
        //1 + (2 + (3 + 4)) -> foldRight
        //((1 + 2) + 3) + 4 -> foldLeft
        val distanceForward = pe.read.cigarElements.drop(1 + pe.cigarElementIndex).foldLeft((true, distanceToThisElementEnd))((keepLookingSum, element) => {
          val (keepLooking, basesTraversed) = keepLookingSum
          if (keepLooking && element.getOperator != cigarOperator)
            (true, basesTraversed + element.getLength)
          else
            (false, basesTraversed)
        })
        val distanceReversed = pe.read.cigarElements.take(pe.cigarElementIndex).foldRight((true, distanceToThisElementBegin))((element, keepLookingSum) => {
          val (keepLooking, basesTraversed) = keepLookingSum
          if (keepLooking && element.getOperator != cigarOperator)
            (true, basesTraversed + element.getLength)
          else
            (false, basesTraversed)
        })

        (distanceForward, distanceReversed) match {
          case ((false, x), (false, y)) => Some(min(x, y))
          case ((false, x), (true, _))  => Some(x)
          case ((true, _), (false, y))  => Some(y)
          case ((true, _), (true, _))   => None
        }

      }
      if ((pe.isInsertion && findInsertions) || (pe.isDeletion && !findInsertions))
        Some(0)
      else {
        if (findInsertions)
          distanceToCigarElement(CigarOperator.INSERTION)
        else
          distanceToCigarElement(CigarOperator.DELETION)
      }
    }

    def mutectHeuristicFiltersPreDbLookup(call: CalledMutectSomaticAllele,
                                          minThetaForPowerCalc: Int = DefaultMutectArgs.minThetaForPowerCalc,
                                          maxGapEventsThresholdForPointMutations: Int = DefaultMutectArgs.maxGapEventsThresholdForPointMutations,
                                          minPassStringentFiltersTumor: Double = DefaultMutectArgs.minPassStringentFiltersTumor,
                                          maxMapq0Fraction: Double = DefaultMutectArgs.maxMapq0Fraction,
                                          minPhredSupportingMutant: Int = DefaultMutectArgs.minPhredSupportingMutant,
                                          maxNormalSupportingFracToTriggerQscoreCheck: Double = DefaultMutectArgs.maxNormalSupportingFracToTriggerQscoreCheck,
                                          maxNormalQscoreSumSupportingMutant: Int = DefaultMutectArgs.maxNormalQscoreSumSupportingMutant,
                                          minMedianDistanceFromReadEnd: Int = DefaultMutectArgs.minMedianDistanceFromReadEnd,
                                          minMedianAbsoluteDeviationOfAlleleInRead: Int = DefaultMutectArgs.minMedianAbsoluteDeviationOfAlleleInRead): Boolean = {

      val passIndel: Boolean = call.mutectEvidence.nInsertions < maxGapEventsThresholdForPointMutations && call.mutectEvidence.nDeletions < maxGapEventsThresholdForPointMutations || call.length > 1

      val passStringentFilters = call.mutectEvidence.heavilyFilteredDepth / call.tumorVariantEvidence.readDepth.toDouble > minPassStringentFiltersTumor

      val passMaxNormalSupport = call.mutectEvidence.filteredNormalAltDepth.toDouble / call.mutectEvidence.filteredNormalDepth.toDouble <= maxNormalSupportingFracToTriggerQscoreCheck ||
        call.mutectEvidence.normalAltQscoreSum < maxNormalQscoreSumSupportingMutant

      val passMapq0Filter = call.mutectEvidence.tumorMapq0Depth.toDouble / call.tumorVariantEvidence.readDepth.toDouble <= maxMapq0Fraction &&
        call.mutectEvidence.normalMapq0Depth.toDouble / call.tumorVariantEvidence.readDepth.toDouble <= maxMapq0Fraction

      val passMaxMapqAlt = call.mutectEvidence.maxAltQuality >= minPhredSupportingMutant

      val passingStrandBias = (call.mutectEvidence.powerPos < 0.9 || call.mutectEvidence.lodPos >= minThetaForPowerCalc) &&
        (call.mutectEvidence.powerNeg < 0.9 || call.mutectEvidence.lodNeg >= minThetaForPowerCalc)

      // Only pass mutations that do not cluster at the ends of reads
      val passEndClustering = (call.mutectEvidence.forwardMad > minMedianAbsoluteDeviationOfAlleleInRead || call.mutectEvidence.forwardMedian > minMedianAbsoluteDeviationOfAlleleInRead) &&
        (call.mutectEvidence.reverseMad > minMedianAbsoluteDeviationOfAlleleInRead || call.mutectEvidence.reverseMedian > minMedianAbsoluteDeviationOfAlleleInRead)

      (passIndel && passStringentFilters && passMapq0Filter &&
        passMaxMapqAlt && passMaxNormalSupport && passEndClustering &&
        passingStrandBias)
    }

    def findPotentialVariantAtLocus(rawTumorPileup: Pileup,
                                    rawNormalPileup: Pileup,
                                    minAlignmentQuality: Int = DefaultMutectArgs.minAlignmentQuality,
                                    minBaseQuality: Int = DefaultMutectArgs.minBaseQuality,
                                    tumorLODThreshold: Double = DefaultMutectArgs.tumorLODThreshold,
                                    maxPhredSumMismatchingBases: Int = DefaultMutectArgs.maxPhredSumMismatchingBases,
                                    indelNearnessThresholdForPointMutations: Int = DefaultMutectArgs.indelNearnessThresholdForPointMutations,
                                    maxFractionBasesSoftClippedTumor: Double = DefaultMutectArgs.maxFractionBasesSoftClippedTumor,
                                    errorForPowerCalculations: Double = DefaultMutectArgs.errorForPowerCalculations,
                                    //contamFrac: Option[Double] = None,
                                    // TODO swap M0 for Mcontam model, basically test alleleFreq vs contamFreq rather than vs 0.0
                                    minThetaForPowerCalc: Int = DefaultMutectArgs.minThetaForPowerCalc,
                                    maxReadDepth: Int = Int.MaxValue): Seq[CalledMutectSomaticAllele] = {
      val model = MutectLogOdds
      val somaticModel = MutectSomaticLogOdds

      val mapqAndBaseqFilteredNormalPileup = mapqBaseqFilteredPu(rawNormalPileup, minBaseQuality, minAlignmentQuality)
      val mapqAndBaseqFilteredTumorPileup = mapqBaseqFilteredPu(rawTumorPileup, minBaseQuality, minAlignmentQuality)

      val mapqBaseqAndOverlappingFragmentFilteredNormalPileup = overlappingFragmentFilteredPileup(mapqAndBaseqFilteredNormalPileup)
      val mapqBaseqAndOverlappingFragmentFilteredTumorPileup = overlappingFragmentFilteredPileup(mapqAndBaseqFilteredTumorPileup)

      // For now, we skip loci that have no reads mapped. We may instead want to emit NoCall in this case.
      if (mapqBaseqAndOverlappingFragmentFilteredTumorPileup.elements.isEmpty
        || mapqBaseqAndOverlappingFragmentFilteredNormalPileup.elements.isEmpty
        || rawNormalPileup.depth > maxReadDepth // skip abnormally deep pileups
        || rawTumorPileup.depth > maxReadDepth
        || mapqBaseqAndOverlappingFragmentFilteredTumorPileup.referenceDepth == mapqBaseqAndOverlappingFragmentFilteredTumorPileup.depth // skip computation if no alternate reads
        )
        return Seq.empty

      val heavilyFilteredTumorPu = heavilyFilteredPu(mapqBaseqAndOverlappingFragmentFilteredTumorPileup,
        maxFractionBasesSoftClippedTumor, maxPhredSumMismatchingBases)
      val alleles = heavilyFilteredTumorPu.distinctAlleles.toSet
      val heavilyFilteredTumorPuElements = heavilyFilteredTumorPu.elements

      //
      val rankedAlts: Seq[(Double, Allele)] =
        alleles.filter(_.isVariant).map { alt =>
          {
            val logOdds = model.logOdds(Bases.basesToString(alt.refBases),
              Bases.basesToString(alt.altBases),
              heavilyFilteredTumorPuElements, None)
            (logOdds, alt)
          }
        }.toSeq.sorted.reverse
      //
      val passingOddsAlts = rankedAlts.filter(oa => oa._1 >= tumorLODThreshold)

      //
      if (passingOddsAlts.length == 1) {
        val alt = passingOddsAlts(0)._2
        val tumorSomaticOdds = passingOddsAlts(0)._1

        val normalNotHet = somaticModel.logOdds(Bases.basesToString(alt.refBases),
          Bases.basesToString(alt.altBases), mapqBaseqAndOverlappingFragmentFilteredNormalPileup.elements, None)

        val nInsertions = heavilyFilteredTumorPuElements.map(ao => if (distanceToNearestReadInsertionOrDeletion(ao, true).getOrElse(Int.MaxValue) <= indelNearnessThresholdForPointMutations) 1 else 0).sum
        val nDeletions = heavilyFilteredTumorPuElements.map(ao => if (distanceToNearestReadInsertionOrDeletion(ao, false).getOrElse(Int.MaxValue) <= indelNearnessThresholdForPointMutations) 1 else 0).sum

        val heavilyFilteredDepth = heavilyFilteredTumorPuElements.length

        val tumorMapq0Depth = rawTumorPileup.elements.filter(_.read.alignmentQuality == 0).length
        val normalMapq0Depth = rawNormalPileup.elements.filter(_.read.alignmentQuality == 0).length

        val onlyTumorMut = heavilyFilteredTumorPuElements.filter(_.allele == alt)
        val onlyTumorMutRaw = rawTumorPileup.elements.filter(_.allele == alt)
        val maxAltQuality = onlyTumorMut.map(_.qualityScore).max

        val normalAltQscoreSum = mapqBaseqAndOverlappingFragmentFilteredNormalPileup.elements.filter(_.allele == alt).map(_.qualityScore).sum
        val filteredNormalAltDepth = mapqBaseqAndOverlappingFragmentFilteredNormalPileup.elements.filter(_.allele == alt).length
        val filteredNormalDepth = mapqBaseqAndOverlappingFragmentFilteredNormalPileup.depth

        val tumorPos = mapqAndBaseqFilteredTumorPileup.elements.filter(_.read.isPositiveStrand)
        val tumorPosAlt = tumorPos.filter(_.allele == alt)
        val tumorNeg = mapqAndBaseqFilteredTumorPileup.elements.filterNot(_.read.isPositiveStrand)
        val tumorNegAlt = tumorNeg.filter(_.allele == alt)

        val alleleFrac = onlyTumorMut.length.toDouble / heavilyFilteredTumorPuElements.length.toDouble

        val tumorPosDepth = tumorPos.size
        val tumorNegDepth = tumorNeg.size
        val tPosFrac = if (tumorPosDepth > 0) tumorPosAlt.size.toDouble / tumorPosDepth.toDouble else 0.0
        val tNegFrac = if (tumorNegDepth > 0) tumorNegAlt.size.toDouble / tumorNegDepth.toDouble else 0.0

        val lodPos = model.logOdds(Bases.basesToString(alt.refBases), Bases.basesToString(alt.altBases), tumorPos, Some(tPosFrac))
        val lodNeg = model.logOdds(Bases.basesToString(alt.refBases), Bases.basesToString(alt.altBases), tumorNeg, Some(tNegFrac))

        val powerPos = calculateStrandPower(tumorPosDepth, alleleFrac, minThetaForPowerCalc)
        val powerNeg = calculateStrandPower(tumorNegDepth, alleleFrac, minThetaForPowerCalc)

        val forwardPositions: Seq[Double] = onlyTumorMut.map(_.readPosition.toDouble)
        val reversePositions: Seq[Double] = onlyTumorMut.map(ao => ao.read.sequence.length - ao.readPosition.toDouble - 1.0)

        val forwardMedian = median(forwardPositions)
        val reverseMedian = median(reversePositions)

        val forwardMad = mad(forwardPositions, forwardMedian)
        val reverseMad = mad(reversePositions, reverseMedian)

        def logOddsToP(logOdds: Double): Double = {
          val odds = Math.pow(logOdds, 10)
          odds / (1 + odds)
        }

        val tumorVariantEvidence = AlleleEvidence(logOddsToP(tumorSomaticOdds), alt, mapqBaseqAndOverlappingFragmentFilteredTumorPileup)
        val normalReferenceEvidence = AlleleEvidence(logOddsToP(normalNotHet), Allele(alt.refBases, alt.refBases), mapqBaseqAndOverlappingFragmentFilteredNormalPileup)
        val mutectEvidence = MutectFilteringEvidence(mutLogOdds = normalNotHet,
          filteredNormalAltDepth = filteredNormalAltDepth,
          filteredNormalDepth = filteredNormalDepth,
          forwardMad = forwardMad,
          reverseMad = reverseMad,
          forwardMedian = forwardMedian,
          reverseMedian = reverseMedian,
          powerPos = powerPos,
          lodPos = lodPos,
          powerNeg = powerNeg,
          lodNeg = lodNeg,
          normalNotHet = normalNotHet,
          normalAltQscoreSum = normalAltQscoreSum,
          maxAltQuality = maxAltQuality,
          tumorMapq0Depth = tumorMapq0Depth,
          normalMapq0Depth = normalMapq0Depth,
          heavilyFilteredDepth = heavilyFilteredDepth,
          nDeletions = nDeletions,
          nInsertions = nInsertions)

        Seq(CalledMutectSomaticAllele(sampleName = rawTumorPileup.sampleName,
          referenceContig = rawTumorPileup.referenceName,
          start = rawTumorPileup.locus,
          allele = alt,
          somaticLogOdds = tumorSomaticOdds,
          tumorVariantEvidence = tumorVariantEvidence,
          normalReferenceEvidence = normalReferenceEvidence,
          mutectEvidence = mutectEvidence,
          length = Math.max(alt.refBases.length, alt.altBases.length)
        ))

      } else {
        Seq.empty
      }
    }
  }

  def calculateStrandPower(depth: Int, f: Double,
                           errorForPowerCalculations: Double = DefaultMutectArgs.errorForPowerCalculations,
                           minThetaForPowerCalc: Int = DefaultMutectArgs.minThetaForPowerCalc): Double = {
    /* The power to detect a mutant is a function of depth, and the mutant allele fraction (unstranded).
        Basically you assume that the probability of observing a base error is uniform and 0.001 (phred score of 30).
        You see how many reads you require to pass the LOD tumorLODThreshold of 2.0, and then you calculate the binomial
        probability of observing that many or more reads would be observed given the allele fraction and depth.
        You also correct for the fact that the real result is somewhere between the minimum integer number to pass,
        and the number below it, so you scale your probability at k by 1 - (2.0 - lod_(k-1) )/(lod_(k) - lod_(k-1)).
         */
    if (depth < 1 || f <= 0.0000001) {
      0.0
    } else {

      def getLod(k: Int): Double = {
        val nref = depth - k
        val tf = k / depth.toDouble
        val prefk = nref.toDouble * math.log10(tf * errorForPowerCalculations + (1.0 - tf) * (1.0 - errorForPowerCalculations))
        val paltk = k.toDouble * math.log10(tf * (1.0 - errorForPowerCalculations) + (1.0 - tf) * errorForPowerCalculations)
        val pkm = prefk + paltk
        val pref0 = nref.toDouble * math.log10(1.0 - errorForPowerCalculations)
        val palt0 = k.toDouble * math.log10(errorForPowerCalculations)
        val p0 = pref0 + palt0
        pkm - p0
      }

      @tailrec
      def findMinPassingK(begin: Int, end: Int, passingK: Option[(Int, Double)]): Option[(Int, Double)] = {
        if (begin >= end) passingK
        else {
          val mid = (begin + end) / 2
          val lod = getLod(mid)
          val passingKupdate = if (lod >= minThetaForPowerCalc) Some((mid, lod)) else passingK
          if (lod >= minThetaForPowerCalc && begin < end - 1) findMinPassingK(begin, mid, passingKupdate)
          else if (begin < end - 1) findMinPassingK(mid, end, passingKupdate)
          else passingKupdate
        }
      }

      val kLodOpt = findMinPassingK(1, depth + 1, None)

      if (kLodOpt.isDefined) {
        val (k, lod) = kLodOpt.get
        val probabilities: Array[Double] = LogBinomial.calculateLogProbabilities(math.log(f), depth)
        val binomials = probabilities.drop(k)

        val lodM1 = getLod(k - 1)

        val partialLogPkM1: Double = probabilities(k - 1) + math.log(1.0 - (minThetaForPowerCalc - lodM1) / (lod - lodM1))

        math.exp(LogUtils.sumLogProbabilities(partialLogPkM1 +: binomials))

      } else {
        0.0
      }
    }

  }

  def median(s: Seq[Double]): Double = {
    val (lower, upper) = s.sortWith(_ < _).splitAt(s.size / 2)
    if (s.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head
  }

  def mad(s: Seq[Double], m: Double): Double = {
    median(s.map(i => math.abs(i - m)))
  }

}
