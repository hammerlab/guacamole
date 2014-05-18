package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole.{ DelayedMessages, DistributedUtil, Common, Command }
import org.apache.spark.Logging
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.guacamole.pileup.{ PileupElement, Pileup }
import org.bdgenomics.adam.avro.{ ADAMVariant, ADAMContig, ADAMGenotypeAllele, ADAMGenotype }
import scala.collection.JavaConversions
import org.apache.spark.rdd.RDD
import org.kohsuke.args4j.Option
import org.bdgenomics.guacamole.Common.Arguments._
import org.bdgenomics.adam.util.PhredUtils

/**
 * A genotype is an n-ploidy sequence of alleles
 * Each allele represents the possible base (or sequence of bases) on a chromosome
 * i.e. DiploidGenotypes would be represented as Seq('A', 'A'), Seq('A', 'T') ... Seq('T', 'G') ... Seq('T', 'T')
 * Alleles can also be multiple characters as well i.e. Seq("AAA", "T")
 *
 * The ploidy is the number of alleles in a set (this should be equal to the number of chromosomes under consideration)
 *
 */
case class Genotype(alleles: String*) {

  /**
   * ploidy is the number of alleles in the genotype
   * should be same as the number of chromosomes in the species
   */
  val ploidy = alleles.size

  lazy val uniqueAllelesCount = alleles.toSet.size

  def getNonReferenceAlleles(referenceAllele: String): Seq[String] = {
    alleles.filter(_ != referenceAllele)
  }

  /**
   *
   * Counts alleles in this genotype that are not the same as the reference allele
   *
   * @param referenceAllele Reference allele to compare against
   * @return Count of non reference alleles
   */
  def numberOfVariants(referenceAllele: String): Int = {
    getNonReferenceAlleles(referenceAllele).size
  }

  /**
   *
   * Check whether this genotype contains any non-reference alleles for a given reference sequence
   *
   * @param referenceAllele Reference allele to compare against
   * @return True if at least one allele is not the reference
   */
  def isVariant(referenceAllele: String): Boolean = {
    numberOfVariants(referenceAllele) > 0
  }

  /**
   * Transform the alleles in this genotype to the allele enumeration
   * Classifies alleles as Reference or Alternate
   *
   * @param referenceAllele Reference allele to compare against
   * @return Sequence of GenotypeAlleles which are reference, alternate or otheralt
   */
  def getGenotypeAlleles(referenceAllele: String): Seq[ADAMGenotypeAllele] = {
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

  private class Arguments extends Base with Output with Reads with DistributedUtil.Arguments {

    @Option(name = "-emit-ref", usage = "Output homozygous reference calls.")
    var emitRef: Boolean = false
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(args, appName = Some(name))

    val reads = Common.loadReads(args, sc, mapped = true, nonDuplicate = true)
    val loci = Common.loci(args, reads)
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, reads, loci)
    val genotypes: RDD[ADAMGenotype] = DistributedUtil.pileupFlatMap[ADAMGenotype](
      reads,
      lociPartitions,
      pileup => callVariantsAtLocus(pileup).iterator)
    reads.unpersist()
    Common.writeVariants(args, genotypes)
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

        val genotypeLikelihoods = computeLikelihoods(pileup)
        val genotypePosteriorEstimate = genotypeLikelihoods.map(kv => (kv._1, kv._2 * computeGenotypePrior(kv._1)))
        val mostLikelyGenotype = genotypePosteriorEstimate.maxBy(_._2)

        def buildVariants(genotype: Genotype, probability: Double): Seq[ADAMGenotype] = {
          val genotypeAlleles = JavaConversions.seqAsJavaList(genotype.getGenotypeAlleles(pileup.referenceBase.toString))
          genotype.getNonReferenceAlleles(pileup.referenceBase.toString).map(
            variantAllele => {
              val variant = ADAMVariant.newBuilder
                .setPosition(pileup.locus)
                .setReferenceAllele(pileup.referenceBase.toString)
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
   * Possible genotypes are all unique n-tuples of sequenceRead that appear in the pileup
   *
   * @return Sequence of possible genotypes
   */
  def getPossibleGenotypes(pileup: Pileup): Seq[Genotype] = {
    val possibleAlleles = pileup.elements.map(_.sequenceRead).distinct
    val possibleGenotypes =
      for (i <- 0 until possibleAlleles.size; j <- i until possibleAlleles.size)
        yield Genotype(possibleAlleles(i), possibleAlleles(j))
    possibleGenotypes
  }

  /**
   * For each possible genotype based on the pileup sequenceRead compute the likelihood
   *
   * @return Sequence of (Genotype, Likelihood)
   */
  def computeLikelihoods(pileup: Pileup): Seq[(Genotype, Double)] = {
    val possibleGenotypes = getPossibleGenotypes(pileup)
    possibleGenotypes.map(g => (g, computeGenotypeLikelihoods(pileup, g, possibleGenotypes.size - 1)))
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
   *  @return likelihood for genotype based P( bases in pileup | genotype )
   */
  protected def computeGenotypeLikelihoods(pileup: Pileup, genotype: Genotype, numAlternateAlleles: Int): Double = {

    def computeBaseGenotypeLikelihood(element: PileupElement, genotype: Genotype): Double = {
      def computeBaseLikelihood(element: PileupElement, referenceAllele: String): Double = {
        val errorProbability = PhredUtils.phredToErrorProbability(element.qualityScore)
        if (element.sequenceRead == referenceAllele) (1 - errorProbability) else errorProbability
      }

      genotype.alleles.map(referenceBase => computeBaseLikelihood(element, referenceBase)).sum / genotype.ploidy
    }

    pileup.elements.map(computeBaseGenotypeLikelihood(_, genotype)).reduce(_ * _)
  }

  /**
   * Compute prior probability for given genotype
   *
   * @return 1.0 (default uniform prior)
   */
  protected def computeGenotypePrior(genotype: Genotype): Double = 1.0

}