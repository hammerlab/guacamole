package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole._
import org.apache.spark.Logging
import org.bdgenomics.guacamole.Common.Arguments.{ TumorNormalReads, Output }
import org.kohsuke.args4j.{ Option => Opt }
import org.bdgenomics.adam.cli.Args4j
import org.apache.spark.rdd.RDD
import org.bdgenomics.guacamole.concordance.GenotypesEvaluator.GenotypeConcordance
import org.bdgenomics.guacamole.pileup.Pileup
import scala.collection.JavaConversions
import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.guacamole.filters.GenotypeFilter.GenotypeFilterArguments
import org.bdgenomics.guacamole.filters.PileupFilter.PileupFilterArguments
import org.bdgenomics.guacamole.filters.{ SomaticGenotypeFilter, GenotypeFilter, PileupFilter }
import org.bdgenomics.formats.avro.{ ADAMGenotypeAllele, ADAMVariant, ADAMContig, ADAMGenotype }
import org.bdgenomics.guacamole.GenotypeEvidence
import org.bdgenomics.guacamole.concordance.GenotypesEvaluator
import org.bdgenomics.guacamole.filters.SomaticGenotypeFilter.SomaticGenotypeFilterArguments

/**
 * Simple subtraction based somatic variant caller
 *
 * This takes two variant callers, calls variants on tumor and normal independently
 * and outputs the variants in the tumor sample BUT NOT the normal sample
 *
 * This assumes that both read sets only contain a single sample, otherwise we should compare
 * on a sample identifier when joining the genotypes
 *
 */
object SomaticLogOddsVariantCaller extends Command with Serializable with Logging {
  override val name = "logodds-somatic"
  override val description = "call somatic variants using a two independent caller on tumor and normal"

  private class Arguments extends DistributedUtil.Arguments with Output with SomaticGenotypeFilterArguments with PileupFilterArguments with TumorNormalReads {

    @Opt(name = "-snvWindowRange", usage = "Number of bases before and after to check for additional matches or deletions")
    var snvWindowRange: Int = 20

    @Opt(name = "-maxNormalAlternateReadDepth", usage = "Maximum number of alternate base reads the normal sample can have")
    var maxNormalAlternateReadDepth: Int = 3

  }

  override def run(rawArgs: Array[String]): Unit = {

    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(args, appName = Some(name))

    val filters = Read.InputFilters(mapped = true, nonDuplicate = true, hasMdTag = true, passedVendorQualityChecks = true)
    val (tumorReads, normalReads) = Common.loadTumorNormalReadsFromArguments(args, sc, filters)

    assert(tumorReads.sequenceDictionary == normalReads.sequenceDictionary,
      "Tumor and normal samples have different sequence dictionaries. Tumor dictionary: %s.\nNormal dictionary: %s."
        .format(tumorReads.sequenceDictionary, normalReads.sequenceDictionary))

    val snvWindowRange = args.snvWindowRange

    val maxMappingComplexity = args.maxMappingComplexity
    val minAlignmentForComplexity = args.minAlignmentForComplexity

    val filterMultiAllelic = args.filterMultiAllelic
    val minAlignmentQuality = args.minAlignmentQuality
    val maxReadDepth = args.maxTumorReadDepth

    val logOddsThreshold = args.logOdds

    val loci = Common.loci(args, normalReads)
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, tumorReads.mappedReads, normalReads.mappedReads)

    val potentialGenotypes: RDD[CalledSomaticGenotype] = DistributedUtil.pileupFlatMapTwoRDDs[CalledSomaticGenotype](
      tumorReads.mappedReads,
      normalReads.mappedReads,
      lociPartitions,
      skipEmpty = true, // skip empty pileups
      (pileupTumor, pileupNormal) => findPotentialVariantAtLocus(
        pileupTumor,
        pileupNormal,
        logOddsThreshold,
        maxMappingComplexity,
        minAlignmentForComplexity,
        minAlignmentQuality,
        filterMultiAllelic,
        maxReadDepth).iterator)

    potentialGenotypes.persist()
    Common.progress("Computed %,d potential genotypes".format(potentialGenotypes.count))

    val genotypeLociPartitions = DistributedUtil.partitionLociUniformly(args.parallelism, loci)

    val genotypes: RDD[CalledSomaticGenotype] = DistributedUtil.windowFlatMapWithState[CalledSomaticGenotype, CalledSomaticGenotype, Option[String]](
      Seq(potentialGenotypes),
      genotypeLociPartitions,
      true, //skipEmpty
      snvWindowRange.toLong,
      None,
      removeCorrelatedGenotypes)
    genotypes.persist()
    Common.progress("Computed %,d genotypes after regional analysis".format(genotypes.count))

    val filteredGenotypes: RDD[CalledSomaticGenotype] = SomaticGenotypeFilter(genotypes, args)
    Common.progress("Computed %,d genotypes after basic filtering".format(filteredGenotypes.count))

    Common.writeVariantsFromArguments(args, filteredGenotypes.flatMap(CalledGenotype.calledSomaticGenotypeToADAMGenotype(_)))

    DelayedMessages.default.print()
  }

  def removeCorrelatedGenotypes(state: Option[String], genotypeWindows: Seq[SlidingWindow[CalledSomaticGenotype]]): (Option[String], Iterator[CalledSomaticGenotype]) = {
    val genotypeWindow = genotypeWindows(0)
    val locus = genotypeWindow.currentLocus
    val currentGenotypes = genotypeWindow.currentRegions.filter(_.overlapsLocus(locus))

    assert(currentGenotypes.length <= 1, "There cannot be more than one called genotype at the given locus")

    if (currentGenotypes.size == genotypeWindow.currentRegions().size) {
      (None, currentGenotypes.iterator)
    } else {
      (None, Iterator.empty)
    }
  }

  def findPotentialVariantAtLocus(tumorPileup: Pileup,
                                  normalPileup: Pileup,
                                  logOddsThreshold: Int,
                                  maxMappingComplexity: Int = 100,
                                  minAlignmentForComplexity: Int = 1,
                                  minAlignmentQuality: Int = 1,
                                  filterMultiAllelic: Boolean = false,
                                  maxReadDepth: Int = Int.MaxValue): Seq[CalledSomaticGenotype] = {

    val filteredNormalPileup = PileupFilter(normalPileup,
      filterMultiAllelic,
      maxMappingComplexity = 100,
      minAlignmentForComplexity,
      minAlignmentQuality,
      minEdgeDistance = 0,
      maxPercentAbnormalInsertSize = 100)
    val filteredTumorPileup = PileupFilter(tumorPileup,
      filterMultiAllelic,
      maxMappingComplexity,
      minAlignmentForComplexity,
      minAlignmentQuality,
      minEdgeDistance = 0,
      maxPercentAbnormalInsertSize = 100)

    // For now, we skip loci that have no reads mapped. We may instead want to emit NoCall in this case.
    if (filteredTumorPileup.elements.isEmpty
      || filteredNormalPileup.elements.isEmpty
      || filteredTumorPileup.referenceDepth == filteredTumorPileup.depth // skip computation if no alternate bases
      || filteredTumorPileup.depth > maxReadDepth // skip  abnormally deep pileups
      || filteredNormalPileup.depth > maxReadDepth)
      return Seq.empty

    val referenceBase = normalPileup.referenceBase
    val tumorSampleName = tumorPileup.elements(0).read.sampleName

    val (alternateBase, tumorVariantLikelihood): (Option[Seq[Byte]], Double) = callVariantInTumor(referenceBase, filteredTumorPileup)
    alternateBase match {
      case Some(alternate) if alternate.nonEmpty => {

        val (alternateReadDepth, alternatePositiveReadDepth) = filteredTumorPileup.alternateReadDepthAndPositiveDepth(alternate)
        val tumorEvidence = GenotypeEvidence(tumorVariantLikelihood,
          filteredTumorPileup.depth,
          alternateReadDepth,
          filteredTumorPileup.positiveDepth,
          alternatePositiveReadDepth)

        val normalLikelihoods =
          BayesianQualityVariantCaller.computeLikelihoods(filteredNormalPileup,
            includeAlignmentLikelihood = false,
            normalize = true).toMap

        val (normalVariantGenotypes, normalReferenceGenotype) = normalLikelihoods.partition(_._1.isVariant(referenceBase))

        val (alternateNormalReadDepth, alternateNormalPositiveReadDepth) = filteredNormalPileup.alternateReadDepthAndPositiveDepth(alternate)
        val normalEvidence = GenotypeEvidence(normalVariantGenotypes.map(_._2).sum,
          filteredNormalPileup.depth,
          alternateNormalReadDepth,
          filteredNormalPileup.positiveDepth,
          alternateNormalPositiveReadDepth)

        val somaticLogOdds = math.log(tumorVariantLikelihood) - math.log(normalVariantGenotypes.map(_._2).sum)
        val normalReferenceLikelihood = normalReferenceGenotype.map(_._2).sum
        val somaticVariantProbability = tumorVariantLikelihood * normalReferenceLikelihood
        val phredScaledSomaticLikelihood = PhredUtils.successProbabilityToPhred(somaticVariantProbability - 1e-10)
        if (somaticLogOdds.isInfinite || phredScaledSomaticLikelihood >= logOddsThreshold) {
          Seq(
            CalledSomaticGenotype(tumorSampleName,
              tumorPileup.referenceName,
              tumorPileup.locus,
              referenceBase,
              alternate,
              somaticLogOdds,
              tumorEvidence,
              normalEvidence)
          )
        } else {
          Seq.empty
        }
      }
      case _ => Seq.empty
    }
  }

  /**
   * Find the most likely genotype in the tumor sample
   * This is either the reference genotype or an heterozygous genotype with some alternate base
   *
   * @param referenceBase Reference base at the current locus
   * @param tumorPileup The pileup of reads at the current locus in the tumor sample
   * @return The alternate base and the likelihood of the most likely variant
   */
  def callVariantInTumor(referenceBase: Byte,
                         tumorPileup: Pileup): (Option[Seq[Byte]], Double) = {

    val tumorLikelihoods = BayesianQualityVariantCaller.computeLikelihoods(tumorPileup,
      includeAlignmentLikelihood = true,
      normalize = true).toMap

    val tumorMostLikelyGenotype = tumorLikelihoods.maxBy(_._2)

    if (tumorMostLikelyGenotype._1.isVariant(referenceBase)) {
      val alternateBase = tumorMostLikelyGenotype._1.getNonReferenceAlleles(referenceBase)(0)
      (Some(alternateBase), tumorMostLikelyGenotype._2)
    } else {
      (None, 1 - tumorMostLikelyGenotype._2)
    }

  }
}

