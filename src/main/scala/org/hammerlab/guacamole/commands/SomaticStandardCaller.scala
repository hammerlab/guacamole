package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.DatabaseVariantAnnotation
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.pileupFlatMapTwoSamples
import org.hammerlab.guacamole.filters.somatic.SomaticGenotypeFilter.SomaticGenotypeFilterArguments
import org.hammerlab.guacamole.filters.somatic.{SomaticAlternateReadDepthFilter, SomaticGenotypeFilter, SomaticReadDepthFilter}
import org.hammerlab.guacamole.likelihood.Likelihood.likelihoodsOfAllPossibleGenotypesFromPileup
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.readsets.ReadSets
import org.hammerlab.guacamole.readsets.args.{ReferenceArgs, TumorNormalReadsArgs}
import org.hammerlab.guacamole.readsets.rdd.{PartitionedRegions, PartitionedRegionsArgs}
import org.hammerlab.guacamole.variants.{Allele, AlleleEvidence, CalledSomaticAllele, GenotypeOutputArgs, GenotypeOutputCaller}
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

  class Arguments
    extends Args
      with TumorNormalReadsArgs
      with PartitionedRegionsArgs
      with SomaticGenotypeFilterArguments
      with GenotypeOutputArgs
      with ReferenceArgs {

    @Args4jOption(name = "--odds", usage = "Minimum log odds threshold for possible variant candidates")
    var oddsThreshold: Int = 20

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
      val oddsThreshold = args.oddsThreshold
      val maxTumorReadDepth = args.maxTumorReadDepth

      val normalSampleName = args.normalSampleName
      val tumorSampleName = args.tumorSampleName

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
              oddsThreshold,
              maxTumorReadDepth
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
                                    oddsThreshold: Int,
                                    maxReadDepth: Int = Int.MaxValue): Seq[CalledSomaticAllele] = {

      // For now, we skip loci that have no reads mapped. We may instead want to emit NoCall in this case.
      if (tumorPileup.elements.isEmpty
        || normalPileup.elements.isEmpty
        || tumorPileup.depth > maxReadDepth // skip abnormally deep pileups
        || normalPileup.depth > maxReadDepth
        || tumorPileup.referenceDepth == tumorPileup.depth // skip computation if no alternate reads
        )
        return Seq.empty

      /**
       * Find the most likely genotype in the tumor sample
       * This is either the reference genotype or an heterozygous genotype with some alternate base
       */
      val genotypesAndLikelihoods =
        likelihoodsOfAllPossibleGenotypesFromPileup(
          tumorPileup,
          includeAlignment = true,
          normalize = true
        )

      if (genotypesAndLikelihoods.isEmpty)
        return Seq.empty

      val (mostLikelyTumorGenotype, mostLikelyTumorGenotypeLikelihood) = genotypesAndLikelihoods.maxBy(_._2)

      // The following lazy vals are only evaluated if mostLikelyTumorGenotype.hasVariantAllele
      lazy val normalLikelihoods =
        likelihoodsOfAllPossibleGenotypesFromPileup(
          normalPileup,
          includeAlignment = false,
          normalize = true
        ).toMap

      lazy val normalVariantGenotypes = normalLikelihoods.filter(_._1.hasVariantAllele)

      lazy val normalVariantsTotalLikelihood = normalVariantGenotypes.values.sum
      lazy val somaticOdds = mostLikelyTumorGenotypeLikelihood / normalVariantsTotalLikelihood

      if (mostLikelyTumorGenotype.hasVariantAllele && somaticOdds * 100 >= oddsThreshold)
        for {
          allele <- mostLikelyTumorGenotype.getNonReferenceAlleles.find(_.altBases.nonEmpty).toSeq

          tumorVariantEvidence = AlleleEvidence(mostLikelyTumorGenotypeLikelihood, allele, tumorPileup)

          refAllele = Allele(allele.refBases, allele.refBases)

          normalReferenceEvidence = AlleleEvidence(1 - normalVariantsTotalLikelihood, refAllele, normalPileup)
        } yield
          CalledSomaticAllele(
            tumorPileup.sampleName,
            tumorPileup.contigName,
            tumorPileup.locus,
            allele,
            math.log(somaticOdds),
            tumorVariantEvidence,
            normalReferenceEvidence
          )
      else
        Seq()
    }
  }
}
