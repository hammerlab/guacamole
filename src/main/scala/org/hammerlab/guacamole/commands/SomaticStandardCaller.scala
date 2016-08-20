package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.DatabaseVariantAnnotation
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.pileupFlatMapTwoSamples
import org.hammerlab.guacamole.filters.pileup.PileupFilter.PileupFilterArguments
import org.hammerlab.guacamole.filters.somatic.SomaticGenotypeFilter.SomaticGenotypeFilterArguments
import org.hammerlab.guacamole.filters.pileup.PileupFilter
import org.hammerlab.guacamole.filters.somatic.{SomaticAlternateReadDepthFilter, SomaticGenotypeFilter, SomaticReadDepthFilter}
import org.hammerlab.guacamole.likelihood.Likelihood
import org.hammerlab.guacamole.logging.DelayedMessages
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.readsets.ReadSets
import org.hammerlab.guacamole.readsets.args.SomaticCallerArgs
import org.hammerlab.guacamole.readsets.io.InputFilters
import org.hammerlab.guacamole.readsets.rdd.PartitionedRegions
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.variants.{Allele, AlleleConversions, AlleleEvidence, CalledSomaticAllele, GenotypeOutputArgs, VariantUtils}
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

  protected class Arguments
    extends SomaticCallerArgs
      with PileupFilterArguments
      with SomaticGenotypeFilterArguments
      with GenotypeOutputArgs {

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
      VariantUtils.validateArguments(args)
      val loci = args.parseLoci(sc.hadoopConfiguration)
      val filters =
        InputFilters(
          overlapsLoci = loci,
          nonDuplicate = true,
          passedVendorQualityChecks = true
        )

      val reference = ReferenceBroadcast(args.referenceFastaPath, sc)

      val (tumorReads, normalReads, contigLengths) =
        ReadSets.loadTumorNormalReads(
          args,
          sc,
          filters
        )

      val filterMultiAllelic = args.filterMultiAllelic
      val minAlignmentQuality = args.minAlignmentQuality
      val maxReadDepth = args.maxTumorReadDepth

      val oddsThreshold = args.oddsThreshold

      val partitionedReads =
        PartitionedRegions(
          tumorReads.mappedReads ++ normalReads.mappedReads,
          loci.result(contigLengths),
          args,
          halfWindowSize = 0
        )

      var potentialGenotypes: RDD[CalledSomaticAllele] =
        pileupFlatMapTwoSamples[CalledSomaticAllele](
          partitionedReads,
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

      VariantUtils.writeVariantsFromArguments(
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
      lazy val normalVariantsTotalLikelihood = normalVariantGenotypes.values.sum
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
            tumorPileup.contigName,
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
