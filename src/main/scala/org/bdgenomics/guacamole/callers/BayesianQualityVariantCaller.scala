package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole._
import org.apache.spark.Logging
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.guacamole.pileup.{ PileupElement, Pileup }
import org.bdgenomics.adam.avro.{ ADAMVariant, ADAMContig, ADAMGenotypeAllele, ADAMGenotype }
import scala.collection.JavaConversions
import org.apache.spark.rdd.RDD
import org.kohsuke.args4j.Option
import org.bdgenomics.guacamole.Common.Arguments._
import org.bdgenomics.adam.util.PhredUtils
import scala.Some
import org.bdgenomics.guacamole.concordance.GenotypesEvaluator
import org.bdgenomics.guacamole.concordance.GenotypesEvaluator.GenotypeConcordance

/**
 * A Genotype is a sequence of alleles of length equal to the ploidy of the organism.
 *
 * A Genotype is for a particular reference locus. Each allele gives the base(s) present on a chromosome at that
 * locus.
 *
 * For example, the possible single-base diploid genotypes are Seq('A', 'A'), Seq('A', 'T') ... Seq('T', 'T').
 * Alleles can also be multiple bases as well, e.g. Seq("AAA", "T")
 *
 */
case class Genotype(alleles: String*) {

  /**
   * The ploidy of the organism is the number of alleles in the genotype.
   */
  val ploidy = alleles.size

  lazy val uniqueAllelesCount = alleles.toSet.size

  def getNonReferenceAlleles(referenceAllele: String): Seq[String] = {
    alleles.filter(_ != referenceAllele)
  }

  /**
   * Counts alleles in this genotype that are not the same as the specified reference allele.
   *
   * @param referenceAllele Reference allele to compare against
   * @return Count of non reference alleles
   */
  def numberOfVariants(referenceAllele: String): Int = {
    getNonReferenceAlleles(referenceAllele).size
  }

  /**
   * Returns whether this genotype contains any non-reference alleles for a given reference sequence.
   *
   * @param referenceAllele Reference allele to compare against
   * @return True if at least one allele is not the reference
   */
  def isVariant(referenceAllele: String): Boolean = {
    numberOfVariants(referenceAllele) > 0
  }

  /**
   * Transform the alleles in this genotype to the ADAM allele enumeration.
   * Classifies alleles as Reference or Alternate.
   *
   * @param referenceAllele Reference allele to compare against
   * @return Sequence of GenotypeAlleles which are Ref, Alt or OtherAlt.
   */
  def getGenotypeAlleles(referenceAllele: String): Seq[ADAMGenotypeAllele] = {
    assume(ploidy == 2)
    val numVariants = numberOfVariants(referenceAllele)
    if (numVariants == 0) {
      Seq(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Ref)
    } else if (numVariants > 0 && uniqueAllelesCount == 1) {
      Seq(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.Alt)
    } else if (numVariants >= 2 && uniqueAllelesCount > 1) {
      Seq(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.OtherAlt)
    } else {
      Seq(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt)
    }
  }
}

/**
 * Simple Bayesian variant caller implementation that uses the base and read quality score
 */
object BayesianQualityVariantCaller extends Command with Serializable with Logging {
  override val name = "uniformbayes"
  override val description = "call variants using a simple quality based probability"

  private class Arguments extends Base with Output with Reads with GenotypeConcordance with DistributedUtil.Arguments {

    @Option(name = "-emit-ref", usage = "Output homozygous reference calls.")
    var emitRef: Boolean = false
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(args, appName = Some(name))

    val (rawReads, sequenceDictionary) = Common.loadReadsFromArguments(args, sc, mapped = true, nonDuplicate = true)
    val mappedReads = rawReads.map(_.getMappedRead)
    mappedReads.persist()
    Common.progress(
      "Loaded %,d mapped non-duplicate reads into %,d partitions.".format(mappedReads.count, mappedReads.partitions.length))

    val loci = Common.loci(args, sequenceDictionary)
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, mappedReads, loci)
    val genotypes: RDD[ADAMGenotype] = DistributedUtil.pileupFlatMap[ADAMGenotype](
      mappedReads,
      lociPartitions,
      pileup => callVariantsAtLocus(pileup).iterator)
    mappedReads.unpersist()
    Common.writeVariants(args, genotypes)

    if (args.truthGenotypesFile != "") GenotypesEvaluator.printGenotypeConcordance(args, genotypes, sc)

    DelayedMessages.default.print()
  }

  /**
   * Computes the genotype and probability at a given locus
   *
   * @param pileup Collection of pileup elements at align to the locus
   * @param emitRef Also return all reference genotypes (default: false)
   *
   * @return Sequence of possible called genotypes for all samples
   */
  def callVariantsAtLocus(
    pileup: Pileup,
    emitRef: Boolean = false): Seq[ADAMGenotype] = {

    // For now, we skip loci that have no reads mapped. We may instead want to emit NoCall in this case.
    if (pileup.elements.isEmpty)
      return Seq.empty

    pileup.bySample.toSeq.flatMap({
      case (sampleName, samplePileup) =>

        val genotypeLikelihoods = computeLogLikelihoods(pileup)
        val genotypePosteriorEstimate = genotypeLikelihoods.map(kv => (kv._1, kv._2 + computeGenotypeLogPrior(kv._1)))
        val mostLikelyGenotype = genotypePosteriorEstimate.maxBy(_._2)

        def buildVariants(genotype: Genotype, probability: Double): Seq[ADAMGenotype] = {
          val genotypeAlleles = JavaConversions.seqAsJavaList(genotype.getGenotypeAlleles(Bases.baseToString(pileup.referenceBase)))
          genotype.getNonReferenceAlleles(Bases.baseToString(pileup.referenceBase)).map(
            variantAllele => {
              val variant = ADAMVariant.newBuilder
                .setPosition(pileup.locus)
                .setReferenceAllele(Bases.baseToString(pileup.referenceBase))
                .setVariantAllele(variantAllele)
                .setContig(ADAMContig.newBuilder.setContigName(pileup.referenceName).build)
                .build
              ADAMGenotype.newBuilder
                .setAlleles(genotypeAlleles)
                .setSampleId(sampleName.toCharArray)
                .setVariant(variant)
                .build
            })
        }
        buildVariants(mostLikelyGenotype._1, mostLikelyGenotype._2)
    })
  }

  /**
   * Generate possible genotypes from a pileup
   * Possible genotypes are all unique n-tuples of sequencedBases that appear in the pileup.
   *
   * @return Sequence of possible genotypes
   */
  def getPossibleGenotypes(pileup: Pileup): Seq[Genotype] = {
    // We prefer to work with Strings than with Array[Byte] for nucleotide sequences, so we convert to Strings as we
    // extract sequences from the Pileup. If this turns into a production variant caller, we may want to use the more
    // efficient Array[Byte] type everywhere.
    val possibleAlleles = pileup.elements.map(e => Bases.basesToString(e.sequencedBases)).distinct
    val possibleGenotypes =
      for (i <- 0 until possibleAlleles.size; j <- i until possibleAlleles.size)
        yield Genotype(possibleAlleles(i), possibleAlleles(j))
    possibleGenotypes
  }

  /**
   * For each possible genotype based on the pileup sequencedBases, compute the likelihood.
   *
   * @return Sequence of (Genotype, Likelihood)
   */
  def computeLikelihoods(pileup: Pileup): Seq[(Genotype, Double)] = {
    val possibleGenotypes = getPossibleGenotypes(pileup)
    possibleGenotypes.map(g => (g, computeGenotypeLikelihoods(pileup, g, possibleGenotypes.size - 1)))
  }

  /**
   * See computeLikelihoods, same computation in log-space
   *
   * @return Sequence of (Genotype, LogLikelihood)
   */
  def computeLogLikelihoods(pileup: Pileup): Seq[(Genotype, Double)] = {
    val possibleGenotypes = getPossibleGenotypes(pileup)
    possibleGenotypes.map(g => (g, computeGenotypeLogLikelihoods(pileup, g, possibleGenotypes.size - 1)))
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
  protected def computeGenotypeLikelihoods(pileup: Pileup, genotype: Genotype, numAlternateAlleles: Int): Double = {

    val depth = pileup.elements.size
    pileup.elements.map(computeBaseGenotypeLikelihood(_, genotype)).reduce(_ * _) / math.pow(genotype.ploidy, depth)
  }

  /**
   * See computeGenotypeLikelihoods, same computation in log-space
   *
   *  @return log likelihood for genotype based P( bases in pileup | genotype )
   */
  protected def computeGenotypeLogLikelihoods(pileup: Pileup, genotype: Genotype, numAlternateAlleles: Int): Double = {

    val depth = pileup.elements.size
    val unnormalizedLikelihood = pileup.elements.map(el => math.log(computeBaseGenotypeLikelihood(el, genotype))).reduce(_ + _)
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
  private def computeBaseGenotypeLikelihood(element: PileupElement, genotype: Genotype): Double = {
    def computeBaseLikelihood(element: PileupElement, referenceAllele: String): Double = {
      val errorProbability = PhredUtils.phredToErrorProbability(element.qualityScore)
      if (Bases.basesToString(element.sequencedBases) == referenceAllele) 1 - errorProbability else errorProbability
    }
    genotype.alleles.map(referenceAllele => computeBaseLikelihood(element, referenceAllele)).sum
  }

  /**
   * Compute prior probability for given genotype, in log-space
   *
   * @return 0.0 (default uniform prior)
   */
  protected def computeGenotypeLogPrior(genotype: Genotype): Double = 0.0

}