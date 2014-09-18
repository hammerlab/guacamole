package org.bdgenomics.guacamole.callers

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.guacamole.Common.Arguments._
import org.bdgenomics.guacamole._
import org.bdgenomics.guacamole.concordance.GenotypesEvaluator
import org.bdgenomics.guacamole.concordance.GenotypesEvaluator.GenotypeConcordance
import org.bdgenomics.guacamole.filters.GenotypeFilter.GenotypeFilterArguments
import org.bdgenomics.guacamole.filters.PileupFilter.PileupFilterArguments
import org.bdgenomics.guacamole.filters.{ GenotypeFilter, QualityAlignedReadsFilter }
import org.bdgenomics.guacamole.pileup.{ Allele, Pileup, PileupElement }
import org.bdgenomics.guacamole.reads.Read
import org.bdgenomics.guacamole.variants._
import org.kohsuke.args4j.Option

/**
 * Simple Bayesian variant caller implementation that uses the base and read quality score
 */
object BayesianQualityVariantCaller extends Command with Serializable with Logging {
  override val name = "uniformbayes"
  override val description = "call variants using a simple quality based probability"

  private class Arguments extends Base
      with Output with Reads with GenotypeConcordance with GenotypeFilterArguments with PileupFilterArguments with DistributedUtil.Arguments {

    @Option(name = "-emit-ref", usage = "Output homozygous reference calls.")
    var emitRef: Boolean = false
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(appName = Some(name))

    val readSet = Common.loadReadsFromArguments(args, sc, Read.InputFilters(mapped = true, nonDuplicate = true))
    readSet.mappedReads.persist()
    Common.progress(
      "Loaded %,d mapped non-duplicate reads into %,d partitions.".format(readSet.mappedReads.count, readSet.mappedReads.partitions.length))

    val loci = Common.loci(args, readSet)
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, readSet.mappedReads)

    val minAlignmentQuality = args.minAlignmentQuality

    val genotypes: RDD[CalledGenotype] = DistributedUtil.pileupFlatMap[CalledGenotype](
      readSet.mappedReads,
      lociPartitions,
      skipEmpty = true, // skip empty pileups
      pileup => callVariantsAtLocus(pileup, minAlignmentQuality).iterator)
    readSet.mappedReads.unpersist()

    val filteredGenotypes = GenotypeFilter(genotypes, args).flatMap(GenotypeConversions.calledGenotypeToADAMGenotype(_))
    Common.writeVariantsFromArguments(args, filteredGenotypes)
    if (args.truthGenotypesFile != "")
      GenotypesEvaluator.printGenotypeConcordance(args, filteredGenotypes, sc)

    DelayedMessages.default.print()
  }

  /**
   * Computes the genotype and probability at a given locus
   *
   * @param pileup Collection of pileup elements at align to the locus
   * @param minAlignmentQuality minimum alignment quality for reads to consider (default: 0)
   * @param emitRef Also return all reference genotypes (default: false)
   *
   * @return Sequence of possible called genotypes for all samples
   */
  def callVariantsAtLocus(
    pileup: Pileup,
    minAlignmentQuality: Int = 0,
    emitRef: Boolean = false): Seq[CalledGenotype] = {

    // For now, we skip loci that have no reads mapped. We may instead want to emit NoCall in this case.
    if (pileup.elements.isEmpty)
      return Seq.empty

    pileup.bySample.toSeq.flatMap({
      case (sampleName, samplePileup) =>
        val filteredPileupElements = QualityAlignedReadsFilter(samplePileup.elements, minAlignmentQuality)
        val genotypeLikelihoods = Pileup(samplePileup.locus, filteredPileupElements).computeLogLikelihoods()
        val mostLikelyGenotype = genotypeLikelihoods.maxBy(_._2)

        def buildVariants(genotype: GenotypeAlleles, probability: Double): Seq[CalledGenotype] = {
          genotype.getNonReferenceAlleles.map(allele => {
            CalledGenotype(
              sampleName,
              samplePileup.referenceName,
              samplePileup.locus,
              allele,
              GenotypeEvidence(
                probability,
                allele,
                samplePileup
              )
            )
          })
        }
        buildVariants(mostLikelyGenotype._1, mostLikelyGenotype._2)

    })
  }
}
