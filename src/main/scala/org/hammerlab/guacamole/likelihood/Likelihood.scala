/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hammerlab.guacamole.likelihood

import breeze.linalg.{ DenseVector, logNormalize, sum, DenseMatrix }
import org.bdgenomics.adam.util.PhredUtils
import org.hammerlab.guacamole.pileup.{ PileupElement, Pileup }
import org.hammerlab.guacamole.variants.{ Allele, Genotype }
import breeze.numerics.{ exp, log }

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
   * @param element the [org.hammerlab.guacamole.pileup.PileupElement]] to consider
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
      Array(genotype),
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

    val alleles = pileup.distinctAlleles
    val genotypes = (for {
      i <- 0 until alleles.size
      j <- i until alleles.size
    } yield Genotype(alleles(i), alleles(j))).toArray
    val likelihoods = likelihoodsOfGenotypes(pileup.elements, genotypes, probabilityCorrect, prior, logSpace, normalize)
    genotypes.zip(likelihoods.toArray)
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
                             genotypes: Array[Genotype],
                             probabilityCorrect: PileupElement => Double = probabilityCorrectIgnoringAlignment,
                             prior: Genotype => Double = uniformPrior,
                             logSpace: Boolean = false,
                             normalize: Boolean = false): DenseVector[Double] = {

    val alleles = genotypes.flatMap(_.alleles).distinct.toIndexedSeq.sorted // the distinct alleles in our genotypes
    val alleleToIndex = alleles.zipWithIndex.toMap // map from allele -> allele index in our alleles sequence.
    val depth = elements.size

    val alleleElementProbabilities = computeAlleleElementProbabilities(elements, alleles.toArray, probabilityCorrect)

    // Calculate likelihoods in log-space. For each genotype, we compute:
    //   sum over elements {
    //      log(probability(allele1, element) + probability(allele2, element))
    //   } + log(prior) - log(ploidy) * depth
    //
    val logLikelihoods: DenseVector[Double] = DenseVector(genotypes.map(genotype => {
      assume(genotype.alleles.size == 2, "Non-diploid genotype not supported")
      val alleleRow1 = alleleElementProbabilities(alleleToIndex(genotype.alleles(0)), ::)
      val alleleRow2 = alleleElementProbabilities(alleleToIndex(genotype.alleles(1)), ::)
      ( //alleleRow1.aggregate(alleleRow2, Functions.plus, Functions.chain(Functions.log, Functions.plus))
        sum(log(alleleRow1 + alleleRow2))
        + math.log(prior(genotype))
        - math.log(2) * depth)
    }))

    // Normalize and/or convert log probs to plain probabilities.
    val possiblyNormalizedLogLikelihoods =
      if (normalize) {
        logNormalize(logLikelihoods)
      } else {
        logLikelihoods
      }

    if (logSpace)
      possiblyNormalizedLogLikelihoods
    else
      exp(possiblyNormalizedLogLikelihoods)
  }

  def computeAlleleElementProbabilities(elements: Seq[PileupElement],
                                        alleles: Array[Allele],
                                        probabilityCorrect: PileupElement => Double = probabilityCorrectIgnoringAlignment): DenseMatrix[Double] = {

    val depth = elements.size

    // alleleElementProbabilities is a two dimensional array where the element at position
    //    (allele index, element index)
    // is
    //    probability(element, allele).
    //
    // where the probability is defined as in the header comment.
    val alleleElementProbabilities = new DenseMatrix[Double](alleles.size, depth)
    for {
      (allele, alleleIndex) <- alleles.zipWithIndex
      (element, elementIndex) <- elements.zipWithIndex
    } {
      val successProbability = probabilityCorrect(element)
      val probability = if (allele == element.allele) successProbability else 1 - successProbability
      alleleElementProbabilities(alleleIndex, elementIndex) = probability
    }

    alleleElementProbabilities
  }

}
