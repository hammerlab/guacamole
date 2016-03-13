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
import htsjdk.samtools.{ CigarElement, CigarOperator }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.DatabaseVariantAnnotation
import org.hammerlab.guacamole.Common.Arguments.SomaticCallerArgs
import org.hammerlab.guacamole.filters.PileupFilter.PileupFilterArguments
import org.hammerlab.guacamole.filters.SomaticGenotypeFilter.SomaticGenotypeFilterArguments
import org.hammerlab.guacamole.filters.{ PileupFilter, SomaticAlternateReadDepthFilter, SomaticGenotypeFilter, SomaticReadDepthFilter }
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

    @Args4jOption(name = "--odds", usage = "Minimum log odds threshold for possible variant candidates")
    var oddsThreshold: Int = 20

    @Args4jOption(name = "--dbsnp-vcf", required = true, usage = "VCF file to identify DBSNP variants")
    var dbSnpVcf: String = ""

    @Args4jOption(name = "--cosmic-vcf", required = true, usage = "VCF file to identify Cosmic variants")
    var cosmicVcf: String = ""

    @Args4jOption(name = "--noisy-muts-vcf", required = true, usage = "VCF file to filter noisy variants")
    var noiseVcf: String = ""

    @Args4jOption(name = "--reference-fasta", required = false, usage = "Local path to a reference FASTA file")
    var referenceFastaPath: String = null

  }

  object DefaultMutectArgs {
    val minAlignmentQuality: Int = 1
    val minBaseQuality: Int = 5
    val threshold: Double = 6.3
    val somDbSnpThreshold: Double = 5.5
    val somNovelThreshold: Double = 2.2
    val maxGapEventsThreshold: Int = 3
    val minPassStringentFiltersTumor: Double = 0.3
    val maxMapq0Fraction: Double = 0.5
    val minPhredSupportingMutant: Int = 20
    val indelNearnessThreshold: Int = 5
    val maxPhredSumMismatchingBases: Int = 100
    val maxFractionBasesSoftClippedTumor: Double = 0.3
    val maxNormalSupportingFracToTriggerQscoreCheck: Double = 0.015
    val maxNormalQscoreSumSupportingMutant: Int = 20
    val minMedianDistanceFromReadEnd: Int = 10
    val minMedianAbsoluteDeviationOfAlleleInRead: Int = 3
    val onlyPointMutations: Boolean = true
    val errorForPowerCalculations: Double = 0.001
    val minThetaForPowerCalc: Int = 20
    val f: Option[Double] = None
    val maxReadDepth: Int = Int.MaxValue

  }

  object Caller extends SparkCommand[Arguments] {
    override val name = "somatic-mutect-like"
    override val description = "call somatic variants using the mutect method on tumor and normal"

    override def run(args: Arguments, sc: SparkContext): Unit = {

      Common.validateArguments(args)
      val loci = Common.lociFromArguments(args)
      import DefaultMutectArgs._

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
              pileupTumor,
              pileupNormal).iterator,
          referenceGenome = reference
        ).filter(mutectHeuristicFiltersPreDbLookup(_))

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

      val filteredGenotypes: RDD[CalledMutectSomaticAllele] = potentialGenotypes.filter(finalMutectDbSnpCosmicNoisyFilter(_, somDbSnpThreshold, somNovelThreshold))

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
          val mateRescueFilter = false //FIXME do we need to implement this?
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
        val distanceForward = pe.read.cigarElements.drop(pe.cigarElementIndex).foldLeft((true, distanceToThisElementEnd))((keepLookingSum, element) => {
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
                                          minThetaForPowerCalc: Int = 20,
                                          maxGapEventsThreshold: Int = 3,
                                          minPassStringentFiltersTumor: Double = 0.3,
                                          maxMapq0Fraction: Double = 0.5,
                                          minPhredSupportingMutant: Int = 20,
                                          maxNormalSupportingFracToTriggerQscoreCheck: Double = 0.015,
                                          maxNormalQscoreSumSupportingMutant: Int = 20,
                                          minMedianDistanceFromReadEnd: Int = 10,
                                          minMedianAbsoluteDeviationOfAlleleInRead: Int = 3): Boolean = {

      val passIndel: Boolean = call.mutectEvidence.nInsertions < maxGapEventsThreshold && call.mutectEvidence.nDeletions < maxGapEventsThreshold

      val passStringentFilters = call.mutectEvidence.heavilyFilteredDepth / call.tumorVariantEvidence.readDepth.toDouble > (1.0 - minPassStringentFiltersTumor)

      // Try replacing this with a strict fischer's exact test?
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
                                    minAlignmentQuality: Int = 1,
                                    minBaseQuality: Int = 5,
                                    threshold: Double = 6.3,
                                    maxPhredSumMismatchingBases: Int = 100,
                                    indelNearnessThreshold: Int = 5,
                                    maxFractionBasesSoftClippedTumor: Double = 0.3,
                                    onlyPointMutations: Boolean = true,
                                    errorForPowerCalculations: Double = 0.001,
                                    f: Option[Double] = None,
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
              heavilyFilteredTumorPuElements, f)
            (logOdds, alt)
          }
        }.toSeq.sorted.reverse
      //
      val passingOddsAlts = rankedAlts.filter(oa => oa._1 >= threshold)

      //
      if (passingOddsAlts.length == 1) {
        val alt = passingOddsAlts(0)._2
        val tumorSomaticOdds = passingOddsAlts(0)._1

        val normalNotHet = somaticModel.logOdds(Bases.basesToString(alt.refBases),
          Bases.basesToString(alt.altBases), mapqAndBaseqFilteredNormalPileup.elements, None)

        val nInsertions = heavilyFilteredTumorPuElements.map(ao => if (distanceToNearestReadInsertionOrDeletion(ao, true).getOrElse(Int.MaxValue) <= indelNearnessThreshold) 1 else 0).sum
        val nDeletions = heavilyFilteredTumorPuElements.map(ao => if (distanceToNearestReadInsertionOrDeletion(ao, false).getOrElse(Int.MaxValue) <= indelNearnessThreshold) 1 else 0).sum

        val heavilyFilteredDepth = heavilyFilteredTumorPuElements.length

        val tumorMapq0Depth = rawTumorPileup.elements.filter(_.read.alignmentQuality == 0).length
        val normalMapq0Depth = rawNormalPileup.elements.filter(_.read.alignmentQuality == 0).length

        val onlyTumorMut = heavilyFilteredTumorPuElements.filter(_.allele == alt)
        val onlyTumorMutRaw = rawTumorPileup.elements.filter(_.allele == alt)
        val maxAltQuality = onlyTumorMut.map(_.qualityScore).max

        val normalAltQscoreSum = mapqAndBaseqFilteredNormalPileup.elements.filter(_.allele == alt).map(_.qualityScore).sum
        val filteredNormalAltDepth = mapqAndBaseqFilteredNormalPileup.elements.filter(_.allele == alt).length
        val filteredNormalDepth = mapqAndBaseqFilteredNormalPileup.depth

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

        val powerPos = calculateStrandPower(tumorPosDepth, alleleFrac)
        val powerNeg = calculateStrandPower(tumorNegDepth, alleleFrac)

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

        val tumorVariantEvidence = AlleleEvidence(logOddsToP(tumorSomaticOdds), alt, mapqAndBaseqFilteredTumorPileup)
        val normalReferenceEvidence = AlleleEvidence(logOddsToP(normalNotHet), Allele(alt.refBases, alt.refBases), mapqAndBaseqFilteredNormalPileup)
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
          mutectEvidence = mutectEvidence
        ))

      } else {
        Seq.empty
      }
    }
  }

  def calculateStrandPower(depth: Int, f: Double, errorForPowerCalculations: Double = 0.001, minThetaForPowerCalc: Int = 20): Double = {
    /* The power to detect a mutant is a function of depth, and the mutant allele fraction (unstranded).
        Basically you assume that the probability of observing a base error is uniform and 0.001 (phred score of 30).
        You see how many reads you require to pass the LOD threshold of 2.0, and then you calculate the binomial
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
