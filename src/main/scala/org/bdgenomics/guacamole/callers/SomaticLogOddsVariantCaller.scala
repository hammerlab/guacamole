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
    @Opt(name = "-log-odds", metaVar = "X", usage = "Make a call if the probability of variant is greater than this value (Phred-scaled)")
    var logOdds: Int = 35

    @Opt(name = "-snvWindowRange", usage = "Number of bases before and after to check for additional matches or deletions")
    var snvWindowRange: Int = 20

    @Opt(name = "-snvCorrelationPercent", usage = "Maximum % of reads that can have additional mismatches or deletions")
    var snvCorrelationPercent: Int = 35

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
    val snvCorrelationPercent = args.snvCorrelationPercent

    val logOddsThreshold = args.logOdds

    val maxMappingComplexity = args.maxMappingComplexity
    val minAlignmentForComplexity = args.minAlignmentForComplexity

    val filterMultiAllelic = args.filterMultiAllelic
    val minAlignmentQuality = args.minAlignmentQuality

    val loci = Common.loci(args, normalReads)
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, tumorReads.mappedReads, normalReads.mappedReads)

    val genotypes: RDD[CalledSomaticGenotype] = DistributedUtil.pileupFlatMapTwoRDDs[CalledSomaticGenotype](
      tumorReads.mappedReads,
      normalReads.mappedReads,
      lociPartitions,
      true, // skip empty pileups
      (pileupTumor, pileupNormal) => callSomaticVariantsAtLocus(
        pileupTumor,
        pileupNormal,
        logOddsThreshold,
        snvWindowRange,
        snvCorrelationPercent,
        maxMappingComplexity,
        minAlignmentForComplexity,
        minAlignmentQuality,
        filterMultiAllelic).iterator)

    genotypes.persist()
    val filteredGenotypes = SomaticGenotypeFilter(genotypes, args).flatMap(CalledGenotype.calledSomaticGenotypeToADAMGenotype(_))
    Common.writeVariantsFromArguments(args, filteredGenotypes)

    Common.progress("Computed %,d genotypes".format(filteredGenotypes.count))

    Common.writeVariantsFromArguments(args, filteredGenotypes)
    DelayedMessages.default.print()
  }

  def callSomaticVariantsAtLocus(tumorPileup: Pileup,
                                 normalPileup: Pileup,
                                 logOddsThreshold: Int,
                                 snvWindowRange: Int = 25,
                                 snvCorrelationPercent: Int = 20,
                                 maxMappingComplexity: Int = 100,
                                 minAlignmentForComplexity: Int = 1,
                                 minAlignmentQuality: Int = 1,
                                 filterMultiAllelic: Boolean = false): Seq[CalledSomaticGenotype] = {

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
      || filteredNormalPileup.elements.isEmpty)
      return Seq.empty

    val referenceBase = Bases.baseToString(normalPileup.referenceBase)
    val tumorSampleName = tumorPileup.elements(0).read.sampleName

    val (alternateBase, tumorVariantLikelihood): (Option[String], Double) = callVariantInTumor(referenceBase, filteredTumorPileup)
    alternateBase match {
      case None => Seq.empty
      case Some(alternate) => {

        if (alternate.length != 1) return Seq.empty

        val (alternateReadDepth, alternatePositiveReadDepth) = filteredTumorPileup.alternateReadDepthAndPositiveDepth(Bases.stringToBases(alternate)(0))

        val (alternateNormalReadDepth, alternateNormalPositiveReadDepth) = filteredTumorPileup.alternateReadDepthAndPositiveDepth(Bases.stringToBases(alternate)(0))

        val normalLikelihoods =
          BayesianQualityVariantCaller.computeLikelihoods(filteredNormalPileup,
            includeAlignmentLikelihood = false,
            normalize = true)

        val (normalVariantGenotypes, normalReferenceGenotype) = normalLikelihoods.partition(_._1.isVariant(referenceBase))

        val somaticLogOdds = math.log(tumorVariantLikelihood) - math.log(normalVariantGenotypes.map(_._2).sum)
        val normalReferenceLikelihood = normalReferenceGenotype.map(_._2).sum

        val somaticVariantProbability = tumorVariantLikelihood * normalReferenceLikelihood
        val phredScaledSomaticLikelihood = PhredUtils.successProbabilityToPhred(somaticVariantProbability - 1e-10)

        if (somaticLogOdds.isInfinite || phredScaledSomaticLikelihood >= logOddsThreshold) {

          val tumorEvidence = GenotypeEvidence(tumorVariantLikelihood,
            filteredTumorPileup.depth,
            alternateReadDepth,
            filteredTumorPileup.positiveDepth,
            alternatePositiveReadDepth)

          val normalEvidence = GenotypeEvidence(tumorVariantLikelihood,
            filteredNormalPileup.depth,
            alternateNormalReadDepth,
            filteredNormalPileup.positiveDepth,
            alternateNormalPositiveReadDepth)
          Seq(
            CalledSomaticGenotype(tumorSampleName,
              tumorPileup.referenceName,
              tumorPileup.locus,
              Bases.stringToBases(referenceBase)(0),
              alternate,
              Genotype(referenceBase, alternate),
              tumorEvidence,
              normalEvidence)
          )
        } else {
          Seq.empty
        }
      }
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
  def callVariantInTumor(referenceBase: String,
                         tumorPileup: Pileup): (Option[String], Double) = {
    def normalPrior(gt: Genotype, hetVariantPrior: Double = 1e-4): Double = {
      val numberVariants = gt.numberOfVariants(referenceBase)
      if (numberVariants > 0) math.pow(hetVariantPrior / gt.uniqueAllelesCount, numberVariants) else 1
    }

    val tumorLikelihoods = BayesianQualityVariantCaller.computeLikelihoods(tumorPileup,
      includeAlignmentLikelihood = true,
      normalize = true,
      prior = normalPrior(_)).toMap

    val tumorMostLikelyGenotype = tumorLikelihoods.maxBy(_._2)

    if (tumorMostLikelyGenotype._1.isVariant(referenceBase)) {
      val alternateBase = tumorMostLikelyGenotype._1.getNonReferenceAlleles(referenceBase)(0)
      (Some(alternateBase), tumorMostLikelyGenotype._2)
    } else {
      (None, 1 - tumorMostLikelyGenotype._2)
    }

  }

}

