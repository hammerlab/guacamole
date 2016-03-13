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

import htsjdk.samtools.{ CigarElement, CigarOperator }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.DatabaseVariantAnnotation
import org.hammerlab.guacamole.Common.Arguments.SomaticCallerArgs
import org.hammerlab.guacamole.likelihood._
import org.hammerlab.guacamole.pileup.{ Pileup, PileupElement }
import org.hammerlab.guacamole.reads.{ MappedRead, Read }
import org.hammerlab.guacamole.variants.{ CalledMutectSomaticAllele, _ }
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole._
import org.kohsuke.args4j.{ Option => Args4jOption }

import scala.annotation.tailrec

/**
 * Implementation of a Mutect like algorithm. Please see docs/README_MutectLikeSomatic.md for more information.
 */
object SomaticMutectLike {

  protected class Arguments extends SomaticCallerArgs with Serializable {

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

    @Args4jOption(name = "--minLodForPowerCalc", usage = "Assumed theta in power calculations")
    var minLodForPowerCalc: Double = DefaultMutectArgs.minLodForPowerCalc

    @Args4jOption(name = "--maxAltAllelesInNormalFilter", usage = "Assumed theta in power calculations")
    var maxAltAllelesInNormalFilter: Int = DefaultMutectArgs.maxAltAllelesInNormalFilter

    @Args4jOption(name = "--contamFrac", usage = "Fraction of contaminating reads in normal/tumor samples")
    var contamFrac: Double = DefaultMutectArgs.contamFrac

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
    val maxNormalSupportingFracToTriggerQscoreCheck: Double = 0.03
    val maxNormalQscoreSumSupportingMutant: Int = 20
    val minMedianDistanceFromReadEnd: Int = 10
    val minMedianAbsoluteDeviationOfAlleleInRead: Int = 3
    val errorForPowerCalculations: Double = 0.001
    val minLodForPowerCalc: Double = 2.0d
    val contamFrac: Double = 0.0
    val maxAltAllelesInNormalFilter: Int = 2
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
          requireMDTagsOnMappedReads = false,
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
              minLodForPowerCalc = args.minLodForPowerCalc,
              contamFrac = args.contamFrac,
              maxReadDepth = args.maxReadDepth).iterator,
          referenceGenome = reference
        ).filter(c => mutectHeuristicFiltersPreDbLookup(call = c,
            minLodForPowerCalc = args.minLodForPowerCalc,
            maxGapEventsThresholdForPointMutations = args.maxGapEventsThresholdForPointMutations,
            minPassStringentFiltersTumor = args.minPassStringentFiltersTumor,
            maxMapq0Fraction = args.maxMapq0Fraction,
            minPhredSupportingMutant = args.minPhredSupportingMutant,
            maxNormalSupportingFracToTriggerQscoreCheck = args.maxNormalSupportingFracToTriggerQscoreCheck,
            maxNormalQscoreSumSupportingMutant = args.maxNormalQscoreSumSupportingMutant,
            minMedianDistanceFromReadEnd = args.minMedianDistanceFromReadEnd,
            minMedianAbsoluteDeviationOfAlleleInRead = args.minMedianAbsoluteDeviationOfAlleleInRead,
            maxAltAllelesInNormalFilter = args.maxAltAllelesInNormalFilter))

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

    /**
     * A filter to apply to a CalledSomaticAllele based on dbsnp/cosmic/noisy site overlap
     * @param somAllele The CalledMutectSomaticAllele to test
     * @param somDbSnpThreshold dbsnp log odds threshold
     * @param somNovelThreshold novel log odds threshold
     * @return True if the site passes the final somatic calling threshold given dbsnp overlap
     *           and also overlap with known problematic regions (noise filter). False otherwise
     */
    def finalMutectDbSnpCosmicNoisyFilter(somAllele: CalledMutectSomaticAllele,
                                          somDbSnpThreshold: Double,
                                          somNovelThreshold: Double): Boolean = {
      val recurrentMutSite = somAllele.cosOverlap.getOrElse(false)
      val noisyMutSite = somAllele.noiseOverlap.getOrElse(false)
      val dbSNPsite = somAllele.rsID.isDefined
      val passSomatic: Boolean = ((dbSNPsite && somAllele.mutectEvidence.normalNotHet >= somDbSnpThreshold) ||
        (!dbSNPsite && somAllele.mutectEvidence.normalNotHet >= somNovelThreshold))
      val passNoiseFilter: Boolean = (recurrentMutSite || !noisyMutSite)

      passSomatic && passNoiseFilter
    }

    /**
     * Filter a pileup based on a minimal base quality, and read mapping quality
     * @param pileup Pileup to filter
     * @param minBaseQuality
     * @param minAlignmentQuality
     * @return Filtered Pileup with failing elements removed
     */
    def mapqBaseqFilteredPu(pileup: Pileup, minBaseQuality: Int, minAlignmentQuality: Int): Pileup = {
      Pileup(pileup.referenceName,
        pileup.locus,
        pileup.referenceBase,
        pileup.elements.filter(pileupElement =>
          pileupElement.qualityScore >= minBaseQuality && pileupElement.read.alignmentQuality >= minAlignmentQuality))
    }

    /**
     * The heavy pileup filters (minus the mate rescue filter) as described in the mutect algorithm.
     *
     * @param pileup Pileup to filter
     * @param maxFractionBasesSoftClippedTumor Reads with more than this fraction bases clipped are removed
     * @param maxPhredSumMismatchingBases Reads with more than this phred score of mismatches are removed
     * @return filtered Pileup
     */
    def heavilyFilteredPu(pileup: Pileup, maxFractionBasesSoftClippedTumor: Double, maxPhredSumMismatchingBases: Int): Pileup = {
      Pileup(pileup.referenceName,
        pileup.locus,
        pileup.referenceBase,
        pileup.elements.filterNot(pileupElement => {
          val clippedFilter = isReadHeavilyClipped(pileupElement.read, maxFractionBasesSoftClippedTumor)
          lazy val noisyFilter = pileupElement.read.misMatchQscoreSum.get >= maxPhredSumMismatchingBases
          val mateRescueFilter = false //TODO determine if we want to do XT=M tag filtering, only relevant for BWA
          clippedFilter || noisyFilter || mateRescueFilter
        })
      )
    }
    /**
     * For every overlpping fragment (pairs of reads overlaping the same position) return one of the pair with
     *  the highest base quality score.
     * @param pileup Pileup object to filter
     * @return filtered Pileup object
     */
    def overlappingFragmentFilteredPileup(pileup: Pileup): Pileup = {
      Pileup(pileup.referenceName,
        pileup.locus,
        pileup.referenceBase,
        pileup.elements.groupBy(_.read.name).map(nameSeq => {
          val pileups = nameSeq._2
          pileups.tail.fold(pileups.head)(
            (pileupElement1: PileupElement, pileupElement2: PileupElement) =>
              if (pileupElement1.qualityScore >= pileupElement2.qualityScore) pileupElement1 else pileupElement2
          ) //chose the pileup element with the best quality score
        }).toSeq)
    }

    /**
     * Function to determine if a read is heavily clipped --> more than a specified fraction of bases of this read
     *   are Hard/Soft clipped
     * @param read Read to examine
     * @param maxFractionBasesSoftClippedTumor The fraction of bases in a clipped state to be considered heavily clipped
     * @return True if at least maxFractionBasesSoftClilppedTumor are marked clipped in this read's cigar object
     */
    def isReadHeavilyClipped(read: MappedRead, maxFractionBasesSoftClippedTumor: Double): Boolean = {
      def isClipped(cigarOperator: CigarOperator): Boolean = Set(CigarOperator.SOFT_CLIP, CigarOperator.HARD_CLIP).contains(cigarOperator)

      val cigar = read.cigarElements
      val trimmedBeginning = if (isClipped(cigar.head.getOperator)) cigar.head.getLength else 0
      val trimmedEnd = if (cigar.length > 1 && isClipped(cigar.last.getOperator)) cigar.last.getLength else 0
      val readLen = read.cigarElements.filter(c => Set(CigarOperator.INSERTION, CigarOperator.MATCH_OR_MISMATCH).contains(c.getOperator)).map(_.getLength).sum
      (trimmedBeginning + trimmedEnd) / (readLen + trimmedBeginning + trimmedEnd) >= maxFractionBasesSoftClippedTumor
    }

    /**
     * Find the distance from this pileup element within a read to the nearest insertion (or deletion) within this read
     * @param pileupElement PileupElement to examine
     * @param findInsertions True if you would like to find insertions, False for deletions.
     * @return Optionally the minimal distance, None if no insertions (or deletions) found.
     */
    def distanceToNearestReadInsertionOrDeletion(pileupElement: PileupElement, findInsertions: Boolean): Option[Int] = {
      def readConsumedBases(ce: CigarElement): Int = {
        if (ce.getOperator.consumesReadBases) ce.getLength else 0
      }
      def distanceToCigarElement(cigarOperator: CigarOperator): Option[Int] = {
        val distanceToThisElementBegin = if (pileupElement.cigarElement.getOperator.consumesReadBases) pileupElement.indexWithinCigarElement + 1 else 0
        val distanceToThisElementEnd = if (pileupElement.cigarElement.getOperator.consumesReadBases) (pileupElement.cigarElement.getLength - pileupElement.indexWithinCigarElement) else 0
        //1 + (2 + (3 + 4)) -> foldRight
        //((1 + 2) + 3) + 4 -> foldLeft
        val distanceForward = pileupElement.read.cigarElements.drop(1 + pileupElement.cigarElementIndex).foldLeft((true, distanceToThisElementEnd))((keepLookingSum, element) => {
          val (keepLooking, basesTraversed) = keepLookingSum
          if (keepLooking && element.getOperator != cigarOperator)
            (true, basesTraversed + readConsumedBases(element))
          else
            (false, basesTraversed)
        })
        val distanceReversed = pileupElement.read.cigarElements.take(pileupElement.cigarElementIndex).foldRight((true, distanceToThisElementBegin))((element, keepLookingSum) => {
          val (keepLooking, basesTraversed) = keepLookingSum
          if (keepLooking && element.getOperator != cigarOperator)
            (true, basesTraversed + readConsumedBases(element))
          else
            (false, basesTraversed)
        })

        (distanceForward, distanceReversed) match {
          case ((false, x), (false, y)) => Some(math.min(x, y))
          case ((false, x), (true, _))  => Some(x)
          case ((true, _), (false, y))  => Some(y)
          case ((true, _), (true, _))   => None
        }

      }
      if ((pileupElement.cigarElement.getOperator == CigarOperator.INSERTION && findInsertions) ||
        (pileupElement.cigarElement.getOperator == CigarOperator.DELETION && !findInsertions))
        Some(0)
      else {
        if (findInsertions)
          distanceToCigarElement(CigarOperator.INSERTION)
        else
          distanceToCigarElement(CigarOperator.DELETION)
      }
    }

    /**
     *
     * @param call The CalledSomaticAllele to filter
     * @param minLodForPowerCalc minimal LOD value to consider the strand-specific mutant LOD sufficient
     * @param maxGapEventsThresholdForPointMutations Maximum number of nearby insertions or deletions to consider a point mutation in a problematic region
     * @param minPassStringentFiltersTumor Minimal fraction of tumor reads that pass stringent filters to allow a call
     * @param maxMapq0Fraction Maximum fraction of raw mapq0 reads to consider a call
     * @param minPhredSupportingMutant Minimal phred score support of a mutation
     * @param maxNormalSupportingFracToTriggerQscoreCheck maximal normal supporting fraction to trigger a normal qscore check
     * @param maxNormalQscoreSumSupportingMutant  minimal sum of alternate allele in normal qscores to trigger a failure
     * @param minMedianDistanceFromReadEnd minimum median distance from the end of reads to ensure the mutation is not too close to the ends
     * @param minMedianAbsoluteDeviationOfAlleleInRead minimum median absolute deviation of the mutations to ensure that they are sufficiently unique by position
     * @param maxAltAllelesInNormalFilter Maximum number of alt alleles in the normal sample at or above which a qscore sum check is enforced
     * @return
     */
    def mutectHeuristicFiltersPreDbLookup(call: CalledMutectSomaticAllele,
                                          minLodForPowerCalc: Double = DefaultMutectArgs.minLodForPowerCalc,
                                          maxGapEventsThresholdForPointMutations: Int = DefaultMutectArgs.maxGapEventsThresholdForPointMutations,
                                          minPassStringentFiltersTumor: Double = DefaultMutectArgs.minPassStringentFiltersTumor,
                                          maxMapq0Fraction: Double = DefaultMutectArgs.maxMapq0Fraction,
                                          minPhredSupportingMutant: Int = DefaultMutectArgs.minPhredSupportingMutant,
                                          maxNormalSupportingFracToTriggerQscoreCheck: Double = DefaultMutectArgs.maxNormalSupportingFracToTriggerQscoreCheck,
                                          maxNormalQscoreSumSupportingMutant: Int = DefaultMutectArgs.maxNormalQscoreSumSupportingMutant,
                                          minMedianDistanceFromReadEnd: Int = DefaultMutectArgs.minMedianDistanceFromReadEnd,
                                          minMedianAbsoluteDeviationOfAlleleInRead: Int = DefaultMutectArgs.minMedianAbsoluteDeviationOfAlleleInRead,
                                          maxAltAllelesInNormalFilter: Int = DefaultMutectArgs.maxAltAllelesInNormalFilter): Boolean = {

      val passIndel: Boolean = call.mutectEvidence.nInsertions < maxGapEventsThresholdForPointMutations && call.mutectEvidence.nDeletions < maxGapEventsThresholdForPointMutations || call.length > 1

      val passStringentFilters = call.mutectEvidence.heavilyFilteredDepth / call.tumorVariantEvidence.readDepth.toDouble > minPassStringentFiltersTumor

      val normalAltF = call.mutectEvidence.filteredNormalAltDepth.toDouble / call.mutectEvidence.filteredNormalDepth.toDouble

      val passMaxNormalSupport = if ((normalAltF >= maxNormalSupportingFracToTriggerQscoreCheck ||
        call.mutectEvidence.filteredNormalAltDepth >= maxAltAllelesInNormalFilter) && (call.mutectEvidence.normalAltQscoreSum > maxNormalQscoreSumSupportingMutant)) false else true

      val passMapq0Filter = call.mutectEvidence.tumorMapq0Depth.toDouble / call.tumorVariantEvidence.readDepth.toDouble <= maxMapq0Fraction &&
        call.mutectEvidence.normalMapq0Depth.toDouble / call.tumorVariantEvidence.readDepth.toDouble <= maxMapq0Fraction

      val passMaxMapqAlt = call.mutectEvidence.maxAltQuality >= minPhredSupportingMutant

      val passingStrandBias = (call.mutectEvidence.powerPos < 0.9 || call.mutectEvidence.lodPos >= minLodForPowerCalc) &&
        (call.mutectEvidence.powerNeg < 0.9 || call.mutectEvidence.lodNeg >= minLodForPowerCalc)

      // Only pass mutations that do not cluster at the ends of reads
      val passEndClustering = (call.mutectEvidence.forwardMad > minMedianAbsoluteDeviationOfAlleleInRead || call.mutectEvidence.forwardMedian > minMedianAbsoluteDeviationOfAlleleInRead) &&
        (call.mutectEvidence.reverseMad > minMedianAbsoluteDeviationOfAlleleInRead || call.mutectEvidence.reverseMedian > minMedianAbsoluteDeviationOfAlleleInRead)

      (passIndel && passStringentFilters && passMapq0Filter &&
        passMaxMapqAlt && passMaxNormalSupport && passEndClustering &&
        passingStrandBias)
    }

    /**
     * Given a tumor/normal pileup, returns the raw called mutations, pre somatic filters or any other heuristic filters,
     * but with all necessary information attached for those downstream filters to function efficiently.
     *
     * @param rawTumorPileup tumor pileup should be nearly completely unfiltered (maybe duplication/platform fail is ok)
     * @param rawNormalPileup normal pileup should be nearly completely unfiltered (maybe duplication/platform fail is ok)
     * @param minAlignmentQuality minimal alignment quality for the initial alignment quality filter
     * @param minBaseQuality minimal base quality for the initial base quality filter
     * @param tumorLODThreshold minimum LOD in the tumor sample to consider a mutant
     * @param maxPhredSumMismatchingBases max phred sum of mismatching bases for stringent filters
     * @param indelNearnessThresholdForPointMutations how close is too close for an indel to a read?
     * @param maxFractionBasesSoftClippedTumor maximal fraction of bases that are soft clipped in a tumor
     * @param errorForPowerCalculations assumed base error for power calculations related to strand filtering
     * @param contamFrac fraction of tumor reads known to come from a contaminating source, used as the baseline allelic fraction for LOD calculation rather than 0
     * @param minLodForPowerCalc minimal LOD value for the per-strand LOD calculation, assuming there is sufficient power to call strand-specific LOD
     * @param maxReadDepth maximal read depth to consider this site.
     * @return A Seq (0 or 1 length currently, only calling a single allele per site) with all necessary information for downstream filtering.
     */
    def findPotentialVariantAtLocus(rawTumorPileup: Pileup,
                                    rawNormalPileup: Pileup,
                                    minAlignmentQuality: Int = DefaultMutectArgs.minAlignmentQuality,
                                    minBaseQuality: Int = DefaultMutectArgs.minBaseQuality,
                                    tumorLODThreshold: Double = DefaultMutectArgs.tumorLODThreshold,
                                    maxPhredSumMismatchingBases: Int = DefaultMutectArgs.maxPhredSumMismatchingBases,
                                    indelNearnessThresholdForPointMutations: Int = DefaultMutectArgs.indelNearnessThresholdForPointMutations,
                                    maxFractionBasesSoftClippedTumor: Double = DefaultMutectArgs.maxFractionBasesSoftClippedTumor,
                                    errorForPowerCalculations: Double = DefaultMutectArgs.errorForPowerCalculations,
                                    contamFrac: Double = DefaultMutectArgs.contamFrac,
                                    minLodForPowerCalc: Double = DefaultMutectArgs.minLodForPowerCalc,
                                    maxReadDepth: Int = Int.MaxValue): Seq[CalledMutectSomaticAllele] = {
      val contamModel = MutectContamLogOdds
      val somaticModel = MutectSomaticLogOdds

      lazy val mapqAndBaseqFilteredNormalPileup = mapqBaseqFilteredPu(rawNormalPileup, minBaseQuality, minAlignmentQuality)
      lazy val mapqAndBaseqFilteredTumorPileup = mapqBaseqFilteredPu(rawTumorPileup, minBaseQuality, minAlignmentQuality)

      lazy val mapqBaseqAndOverlappingFragmentFilteredNormalPileup = overlappingFragmentFilteredPileup(mapqAndBaseqFilteredNormalPileup)
      lazy val mapqBaseqAndOverlappingFragmentFilteredTumorPileup = overlappingFragmentFilteredPileup(mapqAndBaseqFilteredTumorPileup)

      // For now, we skip loci that have no reads mapped. We may instead want to emit NoCall in this case.
      if (mapqBaseqAndOverlappingFragmentFilteredTumorPileup.elements.isEmpty
        || mapqBaseqAndOverlappingFragmentFilteredNormalPileup.elements.isEmpty
        || rawNormalPileup.depth > maxReadDepth // skip abnormally deep pileups
        || rawTumorPileup.depth > maxReadDepth
        || mapqBaseqAndOverlappingFragmentFilteredTumorPileup.referenceDepth == mapqBaseqAndOverlappingFragmentFilteredTumorPileup.depth // skip computation if no alternate reads
        ) {
        Seq.empty
      } else {
        lazy val heavilyFilteredTumorPu = heavilyFilteredPu(mapqBaseqAndOverlappingFragmentFilteredTumorPileup,
          maxFractionBasesSoftClippedTumor, maxPhredSumMismatchingBases)
        lazy val alleles = heavilyFilteredTumorPu.distinctAlleles.toSet
        lazy val heavilyFilteredTumorPuElements = heavilyFilteredTumorPu.elements.toVector
        lazy val heavilyFilteredDepth = heavilyFilteredTumorPuElements.length.toDouble
        def getFracHeavilyFiltered(allele: Allele): Double = {
          heavilyFilteredTumorPuElements.count(_.allele == allele) / heavilyFilteredDepth
        }
        //
        val rankedAlts: Seq[(Double, Allele, Double)] =
          alleles.filter(_.isVariant).map { alt =>
            {
              val minFracAltToConsider = math.max(0.005, contamFrac)
              val altFrac = getFracHeavilyFiltered(alt)
              val logOdds =
                if (altFrac > minFracAltToConsider)
                  contamModel.logOdds(Bases.basesToString(alt.refBases),
                    Bases.basesToString(alt.altBases),
                    heavilyFilteredTumorPuElements,
                    math.min(contamFrac, altFrac))
                else
                  0.0
              (logOdds, alt, altFrac)
            }
          }.toSeq.sorted.reverse
        //
        val passingOddsAlts = rankedAlts.filter(logOddsAltTuple => logOddsAltTuple._1 >= tumorLODThreshold)

        //
        if (passingOddsAlts.length == 1) {

          val alt = passingOddsAlts(0)._2
          val tumorSomaticOdds = passingOddsAlts(0)._1
          val alleleFrac = passingOddsAlts(0)._3

          val normalNotHet = somaticModel.logOdds(Bases.basesToString(alt.refBases),
            Bases.basesToString(alt.altBases), mapqBaseqAndOverlappingFragmentFilteredNormalPileup.elements)

          val nInsertions = heavilyFilteredTumorPuElements.map(pileupElement =>
            if (distanceToNearestReadInsertionOrDeletion(pileupElement, true).getOrElse(Int.MaxValue) <=
              indelNearnessThresholdForPointMutations) 1
            else 0).sum
          val nDeletions = heavilyFilteredTumorPuElements.map(pileupElement =>
            if (distanceToNearestReadInsertionOrDeletion(pileupElement, false).getOrElse(Int.MaxValue) <=
              indelNearnessThresholdForPointMutations) 1
            else 0).sum

          val heavilyFilteredDepth = heavilyFilteredTumorPuElements.length

          val tumorMapq0Depth = rawTumorPileup.elements.count(_.read.alignmentQuality == 0)
          val normalMapq0Depth = rawNormalPileup.elements.count(_.read.alignmentQuality == 0)

          val onlyTumorMutHeavyFiltered = heavilyFilteredTumorPuElements.filter(_.allele == alt)
          val maxAltQuality = onlyTumorMutHeavyFiltered.map(_.qualityScore).max

          val normalAlts = mapqBaseqAndOverlappingFragmentFilteredNormalPileup.elements.filter(_.allele == alt)

          val normalAltQscoreSum = normalAlts.map(_.qualityScore).sum
          val filteredNormalAltDepth = mapqBaseqAndOverlappingFragmentFilteredNormalPileup.elements.count(_.allele == alt)
          val filteredNormalDepth = mapqBaseqAndOverlappingFragmentFilteredNormalPileup.depth

          val tumorPos = mapqAndBaseqFilteredTumorPileup.elements.filter(_.read.isPositiveStrand)
          val tumorPosAlt = tumorPos.filter(_.allele == alt)
          val tumorNeg = mapqAndBaseqFilteredTumorPileup.elements.filterNot(_.read.isPositiveStrand)
          val tumorNegAlt = tumorNeg.filter(_.allele == alt)

          val tumorPosDepth = tumorPos.size
          val tumorNegDepth = tumorNeg.size
          val tPosFrac = if (tumorPosDepth > 0) tumorPosAlt.size.toDouble / tumorPosDepth.toDouble else 0.0
          val tNegFrac = if (tumorNegDepth > 0) tumorNegAlt.size.toDouble / tumorNegDepth.toDouble else 0.0

          val lodPos = contamModel.logOdds(Bases.basesToString(alt.refBases), Bases.basesToString(alt.altBases), tumorPos, math.min(tPosFrac, contamFrac))
          val lodNeg = contamModel.logOdds(Bases.basesToString(alt.refBases), Bases.basesToString(alt.altBases), tumorNeg, math.min(tNegFrac, contamFrac))

          val powerPos = cachingCalculatePowerToDetect(tumorPosDepth, alleleFrac, errorForPowerCalculations, minLodForPowerCalc, contamFrac)
          val powerNeg = cachingCalculatePowerToDetect(tumorNegDepth, alleleFrac, errorForPowerCalculations, minLodForPowerCalc, contamFrac)

          val forwardPositions: Seq[Double] = onlyTumorMutHeavyFiltered.map(_.readPosition.toDouble)
          val reversePositions: Seq[Double] = onlyTumorMutHeavyFiltered.map(pileupElement =>
            pileupElement.read.sequence.length - pileupElement.readPosition.toDouble - 1.0)

          val forwardMedian = median(forwardPositions)
          val reverseMedian = median(reversePositions)

          val forwardMad = mad(forwardPositions, forwardMedian)
          val reverseMad = mad(reversePositions, reverseMedian)

          def logOddsToP(logOdds: Double): Double = {
            val odds = Math.pow(logOdds, 10)
            odds / (1 + odds)
          }

          lazy val tumorVariantEvidence =
            AlleleEvidence(logOddsToP(tumorSomaticOdds),
              mapqAndBaseqFilteredTumorPileup.depth,
              tumorPosAlt.length + tumorNegAlt.length,
              tumorPos.length,
              tumorPosAlt.length,
              -1.0,
              -1.0,
              -1.0,
              -1.0,
              -1.0)
          //AlleleEvidence(logOddsToP(tumorSomaticOdds), alt, mapqBaseqAndOverlappingFragmentFilteredTumorPileup)
          lazy val normalReferenceEvidence =
            AlleleEvidence(logOddsToP(normalNotHet),
              mapqAndBaseqFilteredNormalPileup.depth,
              normalAlts.length,
              -1,
              -1,
              -1.0,
              -1.0,
              -1.0,
              -1.0,
              -1.0)
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
  }

  val powerCache: scala.collection.mutable.Map[(Int, Double, Double, Double, Double), Double] =
    scala.collection.mutable.Map.empty[(Int, Double, Double, Double, Double), Double]
  def cachingCalculatePowerToDetect(depth: Int, f: Double, errorForPowerCalculations: Double, minLodForPowerCalc: Double, contam: Double): Double = {
    val key = (depth, f, errorForPowerCalculations, minLodForPowerCalc, contam)
    powerCache.getOrElseUpdate(key, calculatePowerToDetect(depth, f, errorForPowerCalculations, minLodForPowerCalc, contam))
  }
  /**
   * The power to detect a
   *
   * @param depth depth for power calculation
   * @param f fraction of bases alternate for power calculation
   * @param errorForPowerCalculations error rate for power calc
   * @param minLodForPowerCalc minimum LOD to calculate the power for
   * @param contam the rate of contamination, we need to calculate the LOD beyond this contam fraction
   * @return The power to call a site given the above minimums
   */
  def calculatePowerToDetect(depth: Int, f: Double,
                             errorForPowerCalculations: Double = DefaultMutectArgs.errorForPowerCalculations,
                             minLodForPowerCalc: Double = DefaultMutectArgs.minLodForPowerCalc,
                             contam: Double = DefaultMutectArgs.contamFrac): Double = {
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

      def logLikelihoodSimple(nref: Int, nalts: Int, f: Double): Double = {
        val pref = nref.toDouble * math.log10(f * errorForPowerCalculations + (1.0 - f) * (1.0 - errorForPowerCalculations))
        val palt = nalts.toDouble * math.log10(f * (1.0 - errorForPowerCalculations) + (1.0 - f) * errorForPowerCalculations)
        pref + palt
      }

      def getLod(k: Int): Double = {
        val nref = depth - k
        val tf = k / depth.toDouble
        val pkm = logLikelihoodSimple(nref, k, tf)
        val p0 = logLikelihoodSimple(nref, k, math.min(tf, contam))
        pkm - p0
      }

      @tailrec
      def findMinPassingK(begin: Int, end: Int, passingK: Option[(Int, Double)]): Option[(Int, Double)] = {
        if (begin >= end) passingK
        else {
          val mid = (begin + end) / 2
          val lod = getLod(mid)
          val passingKupdate = if (lod >= minLodForPowerCalc) Some((mid, lod)) else passingK
          if (lod >= minLodForPowerCalc && begin < end - 1) findMinPassingK(begin, mid, passingKupdate)
          else if (begin < end - 1) findMinPassingK(mid, end, passingKupdate)
          else passingKupdate
        }
      }

      val kLodOpt = findMinPassingK(1, depth + 1, None)

      if (kLodOpt.isDefined) {
        val (k, lod) = kLodOpt.get
        val probabilities: Array[Double] = LogBinomial.calculateLogProbabilities(math.log(f * (1 - errorForPowerCalculations) + (1 - f) * errorForPowerCalculations), depth)
        val binomials = probabilities.drop(k)

        val lodM1 = getLod(k - 1)

        //val partialLogPkM1: Double = probabilities(k - 1) + math.log(1.0 - (minLodForPowerCalc - lodM1) / (lod - lodM1))
        val lodM1MinLodDiff = (minLodForPowerCalc - lodM1)
        val lodToLodM1Step = (lod - lodM1)

        // if lod is barely more than minLodForPowerCalc then we have something like 0.999/1, we want to add a small slice of that
        // on the other hand if lod >> minLodForPowerCalc then we have something like 0.01/1, we want to add a large slice of that

        val partialLogPkM1 = probabilities(k - 1) + math.log(1.0 - (lodM1MinLodDiff / lodToLodM1Step))
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
