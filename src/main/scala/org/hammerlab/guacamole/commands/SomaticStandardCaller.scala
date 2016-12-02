package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.DatabaseVariantAnnotation
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.pileupFlatMapTwoSamples
import org.hammerlab.guacamole.filters.somatic.SomaticGenotypeFilter
import org.hammerlab.guacamole.filters.somatic.SomaticGenotypeFilter.SomaticGenotypeFilterArguments
import org.hammerlab.guacamole.likelihood.Likelihood
import org.hammerlab.guacamole.likelihood.Likelihood.probabilitiesOfGenotypes
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.readsets.ReadSets
import org.hammerlab.guacamole.readsets.args.{ReferenceArgs, TumorNormalReadsArgs}
import org.hammerlab.guacamole.readsets.rdd.{PartitionedRegions, PartitionedRegionsArgs}
import org.hammerlab.guacamole.variants.{Allele, AlleleEvidence, CalledSomaticAllele, Genotype, GenotypeOutputArgs, GenotypeOutputCaller}
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.math.{exp, max}

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

  class Arguments
    extends Args
      with TumorNormalReadsArgs
      with PartitionedRegionsArgs
      with SomaticGenotypeFilterArguments
      with GenotypeOutputArgs
      with ReferenceArgs {

    @Args4jOption(
      name = "--normal-odds",
      usage = "Minimum log odds threshold for possible normal-sample variant candidates"
    )
    var normalOddsThreshold: Int = 5

    @Args4jOption(
      name = "--tumor-odds",
      usage = "Minimum log odds threshold for possible tumor-sample variant candidates"
    )
    var tumorOddsThreshold: Int = 10

    @Args4jOption(
      name = "--max-normal-alternate-read-depth",
      usage = "Maximum number of alternates in the normal sample"
    )
    var maxNormalAlternateReadDepth: Int = 4

    @Args4jOption(
      name = "--min-tumor-variant-allele-frequency",
      usage = "Minimum VAF at which to test somatic variants, as a percentage"
    )
    var minTumorVariantAlleleFrequency: Int = 3

    @Args4jOption(name = "--dbsnp-vcf", required = false, usage = "VCF file to identify DBSNP variants")
    var dbSnpVcf: String = ""
  }

  object Caller extends GenotypeOutputCaller[Arguments, CalledSomaticAllele] {
    override val name = "somatic-standard"
    override val description = "call somatic variants using independent callers on tumor and normal"

    override def computeVariants(args: Arguments, sc: SparkContext) = {
      val reference = args.reference(sc)

      val (readsets, loci) = ReadSets(sc, args)

      val partitionedReads =
        PartitionedRegions(
          readsets.allMappedReads,
          loci,
          args
        )

      // Destructure `args`' fields here to avoid serializing `args` itself.
      val normalOddsThreshold = args.normalOddsThreshold
      val tumorOddsThreshold = args.tumorOddsThreshold

      val maxTumorReadDepth = args.maxTumorReadDepth

      val normalSampleName = args.normalSampleName
      val tumorSampleName = args.tumorSampleName

      val maxNormalAlternateReadDepth = args.maxNormalAlternateReadDepth
      val minTumorVariantAlleleFrequency = args.minTumorVariantAlleleFrequency / 100.0f

      var potentialGenotypes: RDD[CalledSomaticAllele] =
        pileupFlatMapTwoSamples[CalledSomaticAllele](
          partitionedReads,
          sample1Name = normalSampleName,
          sample2Name = tumorSampleName,
          skipEmpty = true,  // skip empty pileups
          function = (pileupNormal, pileupTumor) =>
            findPotentialVariantAtLocus(
              pileupTumor,
              pileupNormal,
              normalOddsThreshold,
              tumorOddsThreshold,
              maxTumorReadDepth,
              maxNormalAlternateReadDepth,
              minTumorVariantAlleleFrequency

            ).iterator,
          reference = reference
        )

      potentialGenotypes.persist()
      progress("Computed %,d potential genotypes".format(potentialGenotypes.count))

      if (args.dbSnpVcf != "") {
        val adamContext = new ADAMContext(sc)
        val dbSnpVariants = adamContext.loadVariantAnnotations(args.dbSnpVcf)

        potentialGenotypes =
          potentialGenotypes
            .keyBy(_.bdgVariant)
            .leftOuterJoin(dbSnpVariants.rdd.keyBy(_.getVariant))
            .values
            .map {
              case (calledAllele: CalledSomaticAllele, dbSnpVariantOpt: Option[DatabaseVariantAnnotation]) =>
                calledAllele.copy(rsID = dbSnpVariantOpt.map(_.getDbSnpId))
            }
      }

      (
        SomaticGenotypeFilter(potentialGenotypes, args),
        readsets.sequenceDictionary,
        Vector(args.tumorSampleName)
      )
    }

    def findPotentialVariantAtLocus(tumorPileup: Pileup,
                                    normalPileup: Pileup,
                                    normalOddsThreshold: Int,
                                    tumorOddsThreshold: Int,
                                    maxReadDepth: Int = Int.MaxValue,
                                    maxNormalAlternateReadDepth: Int = 5,
                                    minTumorVariantAlleleFrequency: Float = 0.05f): Option[CalledSomaticAllele] = {

      // For now, we skip loci that have no reads mapped. We may instead want to emit NoCall in this case.
      if (tumorPileup.elements.isEmpty
        || normalPileup.elements.isEmpty
        // skip abnormally deep pileups
        || tumorPileup.depth > maxReadDepth
        || normalPileup.depth > maxReadDepth
        || tumorPileup.referenceDepth == tumorPileup.depth // skip computation if no alternate reads
        || normalPileup.depth - normalPileup.referenceDepth > maxNormalAlternateReadDepth
        )
        return None

      val referenceAllele = Allele(tumorPileup.referenceBase, tumorPileup.referenceBase)
      val referenceGenotype = Genotype(Map(referenceAllele -> 1.0))

      val tumorDepth = tumorPileup.depth
      val variantAlleleFractions: Map[Allele, Double] =
        tumorPileup
          .elements
          .withFilter(_.allele.isVariant)
          .map(_.allele)
          .groupBy(identity)
          .map{ case(k, v) => k -> v.size / tumorDepth.toDouble }

      // Compute empirical frequency of alternate allele in the tumor sample
      // for the likelihood computation
      val (mostFrequentVariantAllele, highestFrequency) = variantAlleleFractions.maxBy(_._2)
      val empiricalVariantAlleleFrequency =  max(minTumorVariantAlleleFrequency, highestFrequency)

      // Build a possible genotype where the alternate allele occurs at the
      // observed empirical VAF
      val somaticVariantGenotype =
        Genotype(
          Map(
            referenceAllele -> (1.0 - empiricalVariantAlleleFrequency),
            mostFrequentVariantAllele -> empiricalVariantAlleleFrequency
          )
        )

      val (tumorRefLogProb, tumorAltLogProb) =
        probabilitiesOfGenotypes(
          tumorPileup.elements,
          (referenceGenotype, somaticVariantGenotype),
          prior = Likelihood.uniformPrior,
          includeAlignment = false,
          logSpace = true
        )

      val tumorAltLOD: Double = tumorAltLogProb - tumorRefLogProb

      val germlineVariantGenotype =
        Genotype(
          Map(
            referenceAllele -> 0.5,
            mostFrequentVariantAllele -> 0.5
          )
        )

      val (normalRefLogProb, normalAltLogProb) =
        probabilitiesOfGenotypes(
          normalPileup.elements,
          (referenceGenotype, germlineVariantGenotype),
          prior = Likelihood.uniformPrior,
          includeAlignment = false,
          logSpace = true
        )

      val normalRefLOD: Double = normalRefLogProb - normalAltLogProb

      if (tumorAltLOD > tumorOddsThreshold &&
          normalRefLOD > normalOddsThreshold &&
          mostFrequentVariantAllele.altBases.nonEmpty) {

        val allele = mostFrequentVariantAllele

        val tumorVariantEvidence = AlleleEvidence(exp(-tumorAltLogProb), allele, tumorPileup)
        val normalReferenceEvidence = AlleleEvidence(exp(-normalRefLogProb), referenceAllele, normalPileup)

        Some(
          CalledSomaticAllele(
            tumorPileup.sampleName,
            tumorPileup.contigName,
            tumorPileup.locus,
            allele,
            tumorAltLOD,
            tumorVariantEvidence,
            normalReferenceEvidence
          )
        )
      } else
        None
    }
  }
}
