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
import org.bdgenomics.guacamole.pileup.{ Pileup, PileupElement }
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
    val sc = Common.createSparkContext(args, appName = Some(name))

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
        val referenceBase = samplePileup.referenceBase
        val filteredPileupElements = QualityAlignedReadsFilter(samplePileup.elements, minAlignmentQuality)
        val genotypeLikelihoods = computeLogLikelihoods(Pileup(samplePileup.locus, filteredPileupElements))
        val mostLikelyGenotype = genotypeLikelihoods.maxBy(_._2)

        def buildVariants(genotype: GenotypeAlleles, probability: Double): Seq[CalledGenotype] = {
          genotype.getNonReferenceAlleles.map(alternate => {
            CalledGenotype(
              sampleName,
              samplePileup.referenceName,
              samplePileup.locus,
              referenceBase,
              alternate,
              GenotypeEvidence(
                probability,
                alternate,
                samplePileup
              )
            )
          })

        }
        buildVariants(mostLikelyGenotype._1, mostLikelyGenotype._2)

    })
  }

  /**
   * Generate possible alleles from a pileup
   * Possible alleles are all unique n-tuples of sequencedBases that appear in the pileup.
   *
   * @return Sequence of possible alleles for the genotype
   */
  def getPossibleAlleles(pileup: Pileup): Seq[GenotypeAlleles] = {

    val possibleAlleles = pileup.elements.map(_.sequencedBases).distinct.sorted(AlleleOrdering)
    val possibleGenotypes =
      for (i <- 0 until possibleAlleles.size; j <- i until possibleAlleles.size)
        yield GenotypeAlleles(pileup.referenceBase, possibleAlleles(i), possibleAlleles(j))
    possibleGenotypes
  }

  /**
   * For each possible genotype based on the pileup sequencedBases, compute the likelihood.
   *
   * @return Sequence of (GenotypeAlleles, Likelihood)
   */
  def computeLikelihoods(pileup: Pileup,
                         prior: GenotypeAlleles => Double = computeUniformGenotypePrior,
                         includeAlignmentLikelihood: Boolean = true,
                         normalize: Boolean = false): Seq[(GenotypeAlleles, Double)] = {

    val possibleGenotypes = getPossibleAlleles(pileup)
    val genotypeLikelihoods = pileup.elements.map(
      computeGenotypeLikelihoods(_, possibleGenotypes, includeAlignmentLikelihood))
      .transpose
      .map(l => l.product / math.pow(2, l.length))

    if (normalize) {
      normalizeLikelihoods(possibleGenotypes.zip(genotypeLikelihoods))
    } else {
      possibleGenotypes.zip(genotypeLikelihoods)
    }

  }

  /**
   * See computeLikelihoods, same computation in log-space
   *
   * @return Sequence of (GenotypeAlleles, LogLikelihood)
   */
  def computeLogLikelihoods(pileup: Pileup,
                            prior: GenotypeAlleles => Double = computeUniformGenotypeLogPrior,
                            includeAlignmentLikelihood: Boolean = false): Seq[(GenotypeAlleles, Double)] = {
    val possibleGenotypes = getPossibleAlleles(pileup)
    possibleGenotypes.map(g =>
      (g, prior(g) + computeGenotypeLogLikelihoods(pileup, g, includeAlignmentLikelihood)))
  }

  /**
   *  Compute likelihood for given genotype
   *
   *  Probability of observing bases given the underlying genotype
   *
   *  P( bases in pileup | genotype )
   *  = \product_bases P( base | genotype)
   *  = \product_bases [ \sum_alleles P( base | allele) / num_alleles (aka ploidy) ]
   *  = \product_bases [ \sum_alleles 1 - error_probability if base == allele else error_probability ]  / num_alleles (aka ploidy)
   *
   *  @return likelihood for genotype based on P( bases in pileup | genotype )
   */

  protected def computeGenotypeLikelihoods(element: PileupElement,
                                           genotypes: Seq[GenotypeAlleles],
                                           includeAlignmentLikelihood: Boolean = false): Seq[Double] = {

    val baseCallProbability = PhredUtils.phredToSuccessProbability(element.qualityScore)
    val successProbability = if (includeAlignmentLikelihood) {
      baseCallProbability * element.read.alignmentLikelihood
    } else {
      baseCallProbability
    }

    genotypes.map(genotype =>
      genotype.alleles.map(referenceAllele =>
        if (referenceAllele == element.sequencedBases) successProbability else (1 - successProbability)).sum)

  }

  /**
   * See computeGenotypeLikelihoods, same computation in log-space
   *
   *  @return log likelihood for genotype based P( bases in pileup | genotype )
   */
  protected def computeGenotypeLogLikelihoods(pileup: Pileup,
                                              genotype: GenotypeAlleles,
                                              includeAlignmentLikelihood: Boolean = false): Double = {

    val depth = pileup.elements.size
    val unnormalizedLikelihood =
      pileup.elements
        .map(el => math.log(computeBaseGenotypeLikelihood(el, genotype, includeAlignmentLikelihood)))
        .reduce(_ + _)
    if (genotype.ploidy == 2) {
      unnormalizedLikelihood - depth
    } else {
      unnormalizedLikelihood - math.log(math.pow(genotype.ploidy, depth))
    }
  }

  /**
   *
   * Computes the likelihood of a pileup element and genoty
   *
   * \sum_alleles P( base | allele)
   *
   * @param element pileup element
   * @param genotype genotype to evaluate
   * @return likelihood for genotype based on P( bases in pileup element | genotype )
   */
  private def computeBaseGenotypeLikelihood(element: PileupElement,
                                            genotype: GenotypeAlleles,
                                            includeAlignmentLikelihood: Boolean = false): Double = {
    def computeBaseLikelihood(element: PileupElement, referenceAllele: Seq[Byte]): Double = {
      val baseCallProbability = PhredUtils.phredToErrorProbability(element.qualityScore)
      val errorProbability = if (includeAlignmentLikelihood) {
        baseCallProbability + element.read.alignmentLikelihood
      } else {
        baseCallProbability
      }

      if (element.sequencedBases == referenceAllele) 1 - errorProbability else errorProbability
    }

    genotype.alleles.map(referenceAllele => computeBaseLikelihood(element, referenceAllele)).sum
  }

  /**
   * Compute prior probability for given genotype, in log-space
   *
   * @return 0.0 (default uniform prior)
   */
  protected def computeUniformGenotypeLogPrior(genotype: GenotypeAlleles): Double = 0.0

  /**
   * Compute prior probability for given genotype
   *
   * @return 1.0 (default uniform prior)
   */
  protected def computeUniformGenotypePrior(genotype: GenotypeAlleles): Double = 1.0

  /*
   * Helper function to normalize probabilities
   */
  def normalizeLikelihoods(likelihoods: Seq[(GenotypeAlleles, Double)]): Seq[(GenotypeAlleles, Double)] = {
    val totalLikelihood = likelihoods.map(_._2).sum
    likelihoods.map(genotypeLikelihood => (genotypeLikelihood._1, genotypeLikelihood._2 / totalLikelihood))
  }
}
