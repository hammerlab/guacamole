package org.bdgenomics.guacamole.callers

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.formats.avro.{ Contig, Genotype, GenotypeAllele, Variant }
import org.bdgenomics.guacamole.Common.Arguments._
import org.bdgenomics.guacamole._
import org.bdgenomics.guacamole.concordance.GenotypesEvaluator
import org.bdgenomics.guacamole.concordance.GenotypesEvaluator.GenotypeConcordance
import org.bdgenomics.guacamole.filters.GenotypeFilter.GenotypeFilterArguments
import org.bdgenomics.guacamole.filters.PileupFilter.PileupFilterArguments
import org.bdgenomics.guacamole.filters.{ GenotypeFilter, QualityAlignedReadsFilter }
import org.bdgenomics.guacamole.pileup.{ Pileup, PileupElement }
import org.bdgenomics.guacamole.reads.Read
import org.kohsuke.args4j.Option

import scala.collection.JavaConversions

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
case class GenotypeAlleles(alleles: String*) {

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
  def getGenotypeAlleles(referenceAllele: String): Seq[GenotypeAllele] = {
    assume(ploidy == 2)
    val numVariants = numberOfVariants(referenceAllele)
    if (numVariants == 0) {
      Seq(GenotypeAllele.Ref, GenotypeAllele.Ref)
    } else if (numVariants > 0 && uniqueAllelesCount == 1) {
      Seq(GenotypeAllele.Alt, GenotypeAllele.Alt)
    } else if (numVariants >= 2 && uniqueAllelesCount > 1) {
      Seq(GenotypeAllele.Alt, GenotypeAllele.OtherAlt)
    } else {
      Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)
    }
  }

}

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
      "Loaded %,d mapped non-duplicate reads into %,d partitions.".format(
        readSet.mappedReads.count(), readSet.mappedReads.partitions.length
      )
    )

    val loci = Common.loci(args, readSet)
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, readSet.mappedReads)

    val minAlignmentQuality = args.minAlignmentQuality

    val genotypes: RDD[Genotype] = DistributedUtil.pileupFlatMap[Genotype](
      readSet.mappedReads,
      lociPartitions,
      skipEmpty = true, // skip empty pileups
      pileup => callVariantsAtLocus(pileup, minAlignmentQuality).iterator)
    readSet.mappedReads.unpersist()

    val filteredGenotypes = GenotypeFilter(genotypes, args)
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
    emitRef: Boolean = false): Seq[Genotype] = {

    // For now, we skip loci that have no reads mapped. We may instead want to emit NoCall in this case.
    if (pileup.elements.isEmpty)
      return Seq.empty

    pileup.bySample.toSeq.flatMap({
      case (sampleName, samplePileup) =>
        val filteredPileupElements = QualityAlignedReadsFilter(samplePileup.elements, minAlignmentQuality)
        val genotypeLikelihoods = computeLogLikelihoods(Pileup(samplePileup.locus, filteredPileupElements))
        val mostLikelyGenotype = genotypeLikelihoods.maxBy(_._2)

        def buildVariants(genotype: GenotypeAlleles, probability: Double): Seq[Genotype] = {
          val genotypeAlleles = JavaConversions.seqAsJavaList(genotype.getGenotypeAlleles(Bases.baseToString(pileup.referenceBase)))
          genotype.getNonReferenceAlleles(Bases.baseToString(pileup.referenceBase)).map(
            variantAllele => {
              val variant = Variant.newBuilder
                .setStart(pileup.locus)
                .setReferenceAllele(Bases.baseToString(pileup.referenceBase))
                .setAlternateAllele(variantAllele)
                .setContig(Contig.newBuilder.setContigName(pileup.referenceName).build)
                .build
              Genotype.newBuilder
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
  def getPossibleGenotypes(pileup: Pileup): Seq[GenotypeAlleles] = {
    // We prefer to work with Strings than with Array[Byte] for nucleotide sequences, so we convert to Strings as we
    // extract sequences from the Pileup. If this turns into a production variant caller, we may want to use the more
    // efficient Array[Byte] type everywhere.
    val possibleAlleles = pileup.elements.map(e => Bases.basesToString(e.sequencedBases)).distinct.sorted
    val possibleGenotypes =
      for (i <- 0 until possibleAlleles.size; j <- i until possibleAlleles.size)
        yield GenotypeAlleles(possibleAlleles(i), possibleAlleles(j))
    possibleGenotypes
  }

  /**
   * For each possible genotype based on the pileup sequencedBases, compute the likelihood.
   *
   * @return Sequence of (Genotype, Likelihood)
   */
  def computeLikelihoods(pileup: Pileup,
                         prior: GenotypeAlleles => Double = computeUniformGenotypePrior,
                         includeAlignmentLikelihood: Boolean = true,
                         normalize: Boolean = false): Seq[(GenotypeAlleles, Double)] = {
    val possibleGenotypes = getPossibleGenotypes(pileup)
    val genotypeLikelihoods = possibleGenotypes.map(g =>
      (g, prior(g) * computeGenotypeLikelihoods(pileup, g, possibleGenotypes.size - 1, includeAlignmentLikelihood)))
    if (normalize) {
      normalizeLikelihoods(genotypeLikelihoods)
    } else {
      genotypeLikelihoods
    }

  }

  /**
   * See computeLikelihoods, same computation in log-space
   *
   * @return Sequence of (Genotype, LogLikelihood)
   */
  def computeLogLikelihoods(pileup: Pileup,
                            prior: GenotypeAlleles => Double = computeUniformGenotypeLogPrior,
                            includeAlignmentLikelihood: Boolean = false): Seq[(GenotypeAlleles, Double)] = {
    val possibleGenotypes = getPossibleGenotypes(pileup)
    possibleGenotypes.map(g =>
      (g, prior(g) + computeGenotypeLogLikelihoods(pileup, g, possibleGenotypes.size - 1, includeAlignmentLikelihood)))
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
  protected def computeGenotypeLikelihoods(pileup: Pileup,
                                           genotype: GenotypeAlleles,
                                           numAlternateAlleles: Int,
                                           includeAlignmentLikelihood: Boolean = false): Double = {

    val depth = pileup.elements.size
    pileup.elements
      .map(computeBaseGenotypeLikelihood(_, genotype, includeAlignmentLikelihood))
      .reduce(_ * _) / math.pow(genotype.ploidy, depth)
  }

  /**
   * See computeGenotypeLikelihoods, same computation in log-space
   *
   *  @return log likelihood for genotype based P( bases in pileup | genotype )
   */
  protected def computeGenotypeLogLikelihoods(pileup: Pileup,
                                              genotype: GenotypeAlleles,
                                              numAlternateAlleles: Int,
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
    def computeBaseLikelihood(element: PileupElement, referenceAllele: String): Double = {
      val baseCallProbability = PhredUtils.phredToErrorProbability(element.qualityScore)
      val errorProbability = if (includeAlignmentLikelihood) {
        baseCallProbability + PhredUtils.phredToErrorProbability(element.read.alignmentQuality)
      } else {
        baseCallProbability
      }

      if (Bases.basesToString(element.sequencedBases) == referenceAllele) 1 - errorProbability else errorProbability
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
