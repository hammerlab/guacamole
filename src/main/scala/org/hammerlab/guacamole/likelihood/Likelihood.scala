package org.hammerlab.guacamole.likelihood

import breeze.linalg.{DenseMatrix, DenseVector, logNormalize, sum}
import breeze.numerics.{exp, log}
import org.hammerlab.guacamole.pileup.{Pileup, PileupElement}
import org.hammerlab.guacamole.util.Bases.isStandardBase
import org.hammerlab.guacamole.variants.{Allele, Genotype}

/**
 * Functions for calculating the likelihood of a genotype given some read evidence (pileup elements).
 */
object Likelihood {
  /**
   * A uniform prior on genotypes, i.e. one where all genotypes have equal prior probability.
   *
   * @param genotype The genotype
   * @return the unnormalized prior probability. Plain probability, NOT a log prob.
   */
  def uniformPrior(genotype: Genotype) = 1.0

  /**
   * Calculate likelihoods for all genotypes with any evidence in a [[org.hammerlab.guacamole.pileup.Pileup]].
   *
   * By "possible genotypes" we mean any genotype where both alleles in the genotype match the sequenced bases of at
   * least one element in the Pileup.
   *
   * See [[likelihoodsOfGenotypes]] for argument descriptions.
   *
   * @return A sequence of (genotype, likelihood) pairs.
   */
  def probabilitiesOfAllPossibleGenotypesFromPileup(
    pileup: Pileup,
    includeAlignment: Boolean = false,
    prior: Genotype => Double = uniformPrior,
    logSpace: Boolean = false): Seq[(Genotype, Double)] = {

    val alleles = pileup.distinctAlleles.filter(allele => allele.altBases.forall(isStandardBase))

    // Assume the alleles are equivalent fractions in the genotype
    val genotypes =
      for {
        i <- alleles.indices
        j <- i until alleles.size
        mixture =
          if (i == j)
            Map(alleles(i) -> 1.0)
          else
            Map(alleles(i) -> 0.5, alleles(j) -> 0.5)
      } yield
        Genotype(mixture)

    val likelihoods =
      likelihoodsOfGenotypes(
        pileup.elements,
        genotypes.toArray,
        includeAlignment,
        prior,
        logSpace,
        normalize = true
      )

    genotypes.zip(likelihoods.data)
  }

  /**
   * Calculate likelihoods for a collection of diploid genotypes.
   *
   * We work with multiple genotypes at once to enable an efficient implementation.
   *
   * For each genotype this calculates:
   *
   *  prior(genotype) * product over all elements of {
   *    sum over the two alleles in the genotype {
   *      probability(element, allele) * f_allele
   *    }
   *  }
   *
   * where
   *
   *  probability(element, allele) = probabilityCorrect(element)     if element.allele = allele
   *                                 1 - probabilityCorrect(element) otherwise
   *
   * f_allele is the allele fraction in the genotype
   *
   * probabilityCorrect(element) is a user supplied function that maps pileup elements to the probability that the
   * sequenced bases for that element are correct, for example by considering the base qualities and/or alignment
   * quality.
   *
   * @param elements the [[org.hammerlab.guacamole.pileup.PileupElement]] instances across which the likelihoods are calculated.
   * @param genotypes the genotypes to calculate likelihoods for.
   * @param includeAlignment whether to factor in each element's read's alignment-likelihood to its likelihood of being
   *                         a sequencing error or not.
   * @param prior a function on genotypes that gives the prior probability that genotype is correct. This function should
   *             return a plain probability, not a log prob.
   * @param logSpace if true, the probabilities are returned as log probs.
   * @param normalize if true, the probabilities returned are normalized to sum to 1.
   * @return A sequence of probabilities corresponding to each genotype in the genotypes argument
   */
  private[likelihood] def likelihoodsOfGenotypes(elements: Seq[PileupElement],
                                                 genotypes: Array[Genotype],
                                                 includeAlignment: Boolean,
                                                 prior: Genotype => Double,
                                                 logSpace: Boolean,
                                                 normalize: Boolean): DenseVector[Double] = {

    // the distinct alleles in our genotypes
    val alleles =
      genotypes
        .flatMap(_.alleles)
        .distinct
        .sorted

    // map from allele -> allele index in our alleles sequence.
    val alleleToIndex =
      alleles
        .zipWithIndex
        .toMap

    val alleleElementProbabilities = computeAlleleElementProbabilities(elements, alleles, includeAlignment)

    // Calculate likelihoods in log-space. For each genotype, we compute:
    //   sum over elements {
    //      log(probability(allele1, element) * f1 + probability(allele2, element) * f2)
    //   } + log(prior) - log(ploidy) * depth
    // where f_i is the allele fraction
    val logLikelihoods: DenseVector[Double] =
      DenseVector(
        genotypes.map(genotype => {
          // For each allele, all elements' probabilities of matching that allele, weighted by that allele's VAF.
          val alleleRows =
            genotype.alleleMixture.map {
              case (allele, alleleFraction) =>
                alleleElementProbabilities(alleleToIndex(allele), ::) * alleleFraction
            }

          sum( log( sum(alleleRows) ) ) + math.log(prior(genotype))
        })
      )

    // Normalize and/or convert log probs to plain probabilities.
    val possiblyNormalizedLogLikelihoods =
      if (normalize)
        logNormalize(logLikelihoods)
      else
        logLikelihoods

    if (logSpace)
      possiblyNormalizedLogLikelihoods
    else
      exp(possiblyNormalizedLogLikelihoods)
  }

  /**
   * Public API for [[likelihoodsOfGenotypes]] with exactly two genotypes.
   */
  def probabilitiesOfGenotypes(elements: Seq[PileupElement],
                               genotypes: (Genotype, Genotype),
                               includeAlignment: Boolean,
                               prior: Genotype => Double,
                               logSpace: Boolean): (Double, Double) = {
    val likelihoods =
      likelihoodsOfGenotypes(
        elements,
        Array(genotypes._1, genotypes._2),
        includeAlignment,
        prior,
        logSpace,
        normalize = true
      )

    (likelihoods(0), likelihoods(1))
  }

  /**
   * Compute a per-{allele,element} matrix of probabilities.
   */
  private def computeAlleleElementProbabilities(elements: Seq[PileupElement],
                                                alleles: Array[Allele],
                                                includeAlignment: Boolean): DenseMatrix[Double] = {
    val depth = elements.size

    // alleleElementProbabilities is a two dimensional array where the element at position
    //    (allele index, element index)
    // is
    //    probability(element, allele).
    //
    // where the probability is defined as in the header comment.
    val alleleElementProbabilities = new DenseMatrix[Double](alleles.length, depth)

    for {
      (allele, alleleIndex) <- alleles.zipWithIndex
      (element, elementIndex) <- elements.zipWithIndex
    } {
      val successProbability =
        if (includeAlignment)
          element.probabilityCorrectIncludingAlignment
        else
          element.probabilityCorrectIgnoringAlignment

      val probability =
        if (allele == element.allele)
          successProbability
        else
          1 - successProbability

      alleleElementProbabilities(alleleIndex, elementIndex) = probability
    }

    alleleElementProbabilities
  }
}
