package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole._
import org.apache.spark.Logging
import org.bdgenomics.guacamole.Common.Arguments.{ TumorNormalReads, Output }
import org.bdgenomics.guacamole.genotype.GenotypeAlleles
import org.bdgenomics.guacamole.reads.Read
import org.kohsuke.args4j.{ Option => Opt }
import org.bdgenomics.adam.cli.Args4j
import org.apache.spark.rdd.RDD
import org.bdgenomics.guacamole.concordance.GenotypesEvaluator.GenotypeConcordance
import org.bdgenomics.guacamole.pileup.Pileup
import scala.collection.JavaConversions
import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.guacamole.filters.GenotypeFilter.GenotypeFilterArguments
import org.bdgenomics.guacamole.filters.PileupFilter.PileupFilterArguments
import org.bdgenomics.guacamole.filters.{ PileupFilter, GenotypeFilter }
import org.bdgenomics.formats.avro.{ GenotypeAllele, Variant, Contig, Genotype }

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

  private class Arguments
      extends DistributedUtil.Arguments
      with Output
      with GenotypeConcordance
      with GenotypeFilterArguments
      with PileupFilterArguments
      with TumorNormalReads {
    @Opt(name = "-log-odds", metaVar = "X", usage = "Make a call if the probability of variant is greater than this value (Phred-scaled)")
    var logOdds: Int = 35

    @Opt(name = "-snvWindowRange", usage = "Number of bases before and after to check for additional matches or deletions")
    var snvWindowRange: Int = 20

    @Opt(name = "-snvCorrelationPercent", usage = "Maximum % of reads that can have additional mismatches or deletions")
    var snvCorrelationPercent: Int = 35

    @Opt(name = "-minNormalReadDepth", usage = "Minimum number of reads in the normal sample at a locus")
    var minNormalReadDepth: Int = 3

    @Opt(name = "-maxNormalAlternateReadDepth", usage = "Maximum number of alternate base reads the normal sample can have")
    var maxNormalAlternateReadDepth: Int = 3

  }

  override def run(rawArgs: Array[String]): Unit = {

    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(appName = Some(name))

    val filters = Read.InputFilters(mapped = true, nonDuplicate = true, passedVendorQualityChecks = true)
    val (tumorReads, normalReads) = Common.loadTumorNormalReadsFromArguments(args, sc, filters)

    assert(tumorReads.sequenceDictionary == normalReads.sequenceDictionary,
      "Tumor and normal samples have different sequence dictionaries. Tumor dictionary: %s.\nNormal dictionary: %s."
        .format(tumorReads.sequenceDictionary, normalReads.sequenceDictionary))

    val minAlternateReadDepth = args.minAlternateReadDepth

    val snvWindowRange = args.snvWindowRange
    val snvCorrelationPercent = args.snvCorrelationPercent

    val maxNormalAlternateReadDepth = args.maxNormalAlternateReadDepth
    val minNormalReadDepth = args.minNormalReadDepth

    val logOddsThreshold = args.logOdds

    val maxMappingComplexity = args.maxMappingComplexity
    val minAlignmentForComplexity = args.minAlignmentForComplexity

    val filterMultiAllelic = args.filterMultiAllelic
    val minAlignmentQuality = args.minAlignmentQuality

    val loci = Common.loci(args, normalReads)
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, tumorReads.mappedReads, normalReads.mappedReads)

    val genotypes: RDD[Genotype] = DistributedUtil.pileupFlatMapTwoRDDs[Genotype](
      tumorReads.mappedReads,
      normalReads.mappedReads,
      lociPartitions,
      skipEmpty = true, // skip empty pileups
      (pileupTumor, pileupNormal) => callSomaticVariantsAtLocus(
        pileupTumor,
        pileupNormal,
        logOddsThreshold,
        snvWindowRange,
        snvCorrelationPercent,
        minNormalReadDepth,
        maxNormalAlternateReadDepth,
        minAlternateReadDepth,
        maxMappingComplexity,
        minAlignmentForComplexity,
        minAlignmentQuality,
        filterMultiAllelic).iterator)

    genotypes.persist()
    val filteredGenotypes = GenotypeFilter(genotypes, args)
    Common.progress("Computed %,d genotypes".format(filteredGenotypes.count))

    Common.writeVariantsFromArguments(args, filteredGenotypes)
    DelayedMessages.default.print()
  }

  def callSomaticVariantsAtLocus(tumorPileup: Pileup,
                                 normalPileup: Pileup,
                                 logOddsThreshold: Int,
                                 snvWindowRange: Int = 25,
                                 snvCorrelationPercent: Int = 20,
                                 minNormalReadDepth: Int = 5,
                                 maxNormalAlternateReadDepth: Int = 5,
                                 minAlternateReadDepth: Int = 2,
                                 maxMappingComplexity: Int = 100,
                                 minAlignmentForComplexity: Int = 1,
                                 minAlignmentQuality: Int = 1,
                                 filterMultiAllelic: Boolean = false): Seq[Genotype] = {

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
      || filteredNormalPileup.depth < minNormalReadDepth)
      return Seq.empty

    val referenceBase = normalPileup.referenceBase
    val tumorSampleName = tumorPileup.elements(0).read.sampleName

    val (alternateBase, tumorVariantLikelihood): (Option[Seq[Byte]], Double) = callVariantInTumor(referenceBase, filteredTumorPileup)
    alternateBase match {

      case Some(alternate) => {

        val (alternateReadDepth, alternateForwardReadDepth) = computeDepthAndForwardDepth(filteredTumorPileup, alternate)

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
          buildVariants(
            tumorSampleName,
            normalPileup.referenceName,
            referenceBase,
            normalPileup.locus,
            alternate,
            tumorVariantLikelihood,
            filteredTumorPileup.depth, alternateReadDepth, 0)
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
    def normalPrior(gt: GenotypeAlleles, hetVariantPrior: Double = 1e-4): Double = {
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

  /**
   *
   * Find the number of reads and number of forwards reads that support a given base in a pileup
   *
   * @param pileup pileup of reads at a certain locus
   * @param base base to search for in that pileup
   * @return Number of reads that support the given base and number of reads in the forward direction that support it
   */
  def computeDepthAndForwardDepth(pileup: Pileup, base: Seq[Byte]): (Int, Int) = {
    val baseElements = pileup.elements.view.filter(el => el.sequencedBases == base)
    val readDepth = baseElements.length
    val baseForwardReadDepth = baseElements.count(_.read.isPositiveStrand)
    (readDepth, baseForwardReadDepth)
  }

  def buildVariants(sampleName: String,
                    referenceName: String,
                    referenceBase: Byte,
                    locus: Long,
                    alternateBase: Seq[Byte],
                    probability: Double,
                    readDepth: Int,
                    alternateReadDepth: Int,
                    alternateForwardDepth: Int,
                    delta: Double = 1e-10): Seq[Genotype] = {
    val genotypeAlleles = JavaConversions.seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Alt))
    val variant = Variant.newBuilder
      .setStart(locus)
      .setReferenceAllele(Bases.baseToString(referenceBase))
      .setAlternateAllele(Bases.basesToString(alternateBase))
      .setContig(Contig.newBuilder.setContigName(referenceName).build)
      .build
    Seq(Genotype.newBuilder
      .setAlleles(genotypeAlleles)
      .setGenotypeQuality(PhredUtils.successProbabilityToPhred(probability - delta))
      .setReadDepth(readDepth)
      .setExpectedAlleleDosage(alternateReadDepth.toFloat / readDepth)
      .setSampleId(sampleName.toCharArray)
      .setAlternateReadDepth(alternateReadDepth)
      .setVariant(variant)
      .build)
  }

}

