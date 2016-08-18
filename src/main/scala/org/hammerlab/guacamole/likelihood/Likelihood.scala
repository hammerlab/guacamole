package org.hammerlab.guacamole.likelihood

import cern.colt.matrix.impl.DenseDoubleMatrix2D
import cern.jet.math.Functions
import org.bdgenomics.adam.util.PhredUtils
import org.hammerlab.guacamole.pileup.{Pileup, PileupElement}
import org.hammerlab.guacamole.util.Bases
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
   * One way of defining the likelihood that the sequenced bases in a pileup element are correct.
   *
   * This considers only the base quality scores.
   *
   * @param element the [[PileupElement]] to consider
   * @return the unnormalized likelihood the sequenced bases are correct. Plain probability, NOT a log prob.
   */
  def probabilityCorrectIgnoringAlignment(element: PileupElement): Double = {
    PhredUtils.phredToSuccessProbability(element.qualityScore)
  }

  /**
   * Another way of defining the likelihood that the sequenced bases in a pileup element are correct.
   *
   * This considers both the base quality scores and alignment quality of the corresponding read.
   *
   * @param element the [[org.hammerlab.guacamole.pileup.PileupElement]] to consider
   * @return the unnormalized likelihood the sequenced bases are correct. Plain probability, NOT a log prob.
   */
  def probabilityCorrectIncludingAlignment(element: PileupElement): Double = {
    PhredUtils.phredToSuccessProbability(element.qualityScore) * element.read.alignmentLikelihood
  }

  /**
   * Calculate the likelihood of a single genotype.
   *
   * @see [[likelihoodsOfGenotypes]] for argument descriptions.
   *
   * @return The likelihood for the given genotype.
   */
  def likelihoodOfGenotype(
    elements: Seq[PileupElement],
    genotype: Genotype,
    probabilityCorrect: PileupElement => Double = probabilityCorrectIgnoringAlignment,
    prior: Genotype => Double = uniformPrior,
    logSpace: Boolean = false): Double = {

    val result = likelihoodsOfGenotypes(
      elements,
      Seq(genotype),
      probabilityCorrect,
      prior,
      logSpace,
      normalize = false)
    assert(result.size == 1)
    result(0)
  }

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
  def likelihoodsOfAllPossibleGenotypesFromPileup(
    pileup: Pileup,
    probabilityCorrect: PileupElement => Double = probabilityCorrectIgnoringAlignment,
    prior: Genotype => Double = uniformPrior,
    logSpace: Boolean = false,
    normalize: Boolean = false): Seq[(Genotype, Double)] = {

    val alleles = pileup.distinctAlleles.filter(allele => allele.altBases.forall((Bases.isStandardBase _)))
    val genotypes = for {
      i <- alleles.indices
      j <- i until alleles.size
    } yield Genotype(alleles(i), alleles(j))
    val likelihoods = likelihoodsOfGenotypes(pileup.elements, genotypes, probabilityCorrect, prior, logSpace, normalize)
    genotypes.zip(likelihoods)
  }

  /**
   * Calculate likelihoods for a collection of diploid genotypes.
   *
   * We work with multiple genotypes at once to enable an efficient implementation.
   *
   * For each genotype this calculates:
   *
   *  prior(genotype) / pow(2, depth) * product over all elements of {
   *    sum over the two alleles in the genotype {
   *      probability(element, allele)
   *    }
   *  }
   *
   * where
   *
   *  probability(element, allele) = probabilityCorrect(element)     if element.allele = allele
   *                                 1 - probabilityCorrect(element) otherwise
   *
   * and probabilityCorrect(element) is a user supplied function that maps pileup elements to the probability that the
   * sequenced bases for that element are correct, for example by considering the base qualities and/or alignment
   * quality.
   *
   * @param elements the [[org.hammerlab.guacamole.pileup.PileupElement]] instances across which the likelihoods are calculated.
   * @param genotypes the genotypes to calculate likelihoods for.
   * @param probabilityCorrect a function of a pileup element that gives the probability that the bases sequenced are
   *                           correct. See [[probabilityCorrectIgnoringAlignment]] and
   *                           [[probabilityCorrectIncludingAlignment]] for two reasonable functions to use here.
   *                           This function should return a plain probability, not a log prob.
   * @param prior a function on genotypes that gives the prior probability that genotype is correct. This function should
   *             return a plain probability, not a log prob.
   * @param logSpace if true, the probabilities are returned as log probs.
   * @param normalize if true, the probabilities returned are normalized to sum to 1.
   * @return A sequence of probabilities corresponding to each genotype in the genotypes argument
   */
  def likelihoodsOfGenotypes(elements: Seq[PileupElement],
                             genotypes: Seq[Genotype],
                             probabilityCorrect: PileupElement => Double = probabilityCorrectIgnoringAlignment,
                             prior: Genotype => Double = uniformPrior,
                             logSpace: Boolean = false,
                             normalize: Boolean = false): Seq[Double] = {

    val alleles = genotypes.flatMap(_.alleles).distinct.toIndexedSeq.sorted // the distinct alleles in our genotypes
    val alleleToIndex = alleles.zipWithIndex.toMap // map from allele -> allele index in our alleles sequence.
    val depth = elements.size

    // alleleElementProbabilities is a two dimensional array where the element at position
    //    (allele index, element index)
    // is
    //    probability(element, allele).
    //
    // where the probability is defined as in the header comment.
    val alleleElementProbabilities = new DenseDoubleMatrix2D(alleles.size, depth)
    for {
      (allele, alleleIndex) <- alleles.zipWithIndex
      (element, elementIndex) <- elements.zipWithIndex
    } {
      val successProbability = probabilityCorrect(element)
      val probability = if (allele == element.allele) successProbability else 1 - successProbability
      alleleElementProbabilities.set(alleleIndex, elementIndex, probability)
    }

    // Calculate likelihoods in log-space. For each genotype, we compute:
    //   sum over elements {
    //      log(probability(allele1, element) + probability(allele2, element))
    //   } + log(prior) - log(ploidy) * depth
    //
    val logLikelihoods = genotypes.map(genotype => {
      assume(genotype.alleles.size == 2, "Non-diploid genotype not supported")
      val alleleRow1 = alleleElementProbabilities.viewRow(alleleToIndex(genotype.alleles(0)))
      val alleleRow2 = alleleElementProbabilities.viewRow(alleleToIndex(genotype.alleles(1)))
      (alleleRow1.aggregate(alleleRow2, Functions.plus, Functions.chain(Functions.log, Functions.plus))
        + math.log(prior(genotype))
        - math.log(2) * depth)
    })

    // Normalize and/or convert log probs to plain probabilities.
    val possiblyNormalizedLogLikelihoods = if (normalize) {
      val logTotalLikelihood = math.log(logLikelihoods.map(math.exp).sum)
      logLikelihoods.map(_ - logTotalLikelihood)
    } else {
      logLikelihoods
    }
    if (logSpace)
      possiblyNormalizedLogLikelihoods
    else
      possiblyNormalizedLogLikelihoods.map(math.exp)
  }
}
