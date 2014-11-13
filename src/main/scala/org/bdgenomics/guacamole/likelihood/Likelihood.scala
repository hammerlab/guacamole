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

package org.bdgenomics.guacamole.likelihood

import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.guacamole.pileup.{ PileupElement, Pileup }
import org.bdgenomics.guacamole.variants.{ Allele, Genotype }

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
   * One way of defining the likelihood that the sequenced bases in a [[PileupElement]] are correct.
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
   * Another way of defining the likelihood that the sequenced bases in a [[PileupElement]] are correct.
   *
   * This considers both the base quality scores and alignment quality of the corresponding read.
   *
   * @param element the [[PileupElement]] to consider
   * @return the unnormalized likelihood the sequenced bases are correct. Plain probability, NOT a log prob.
   */
  def probabilityCorrectIncludingAlignment(element: PileupElement): Double = {
    PhredUtils.phredToSuccessProbability(element.qualityScore) * element.read.alignmentLikelihood
  }

  /**
   * Calculate the likelihood of a single genotype.
   *
   * See [[likelihoodsOfGenotypes()]] for argument descriptions.
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
   * Calculate likelihoods for all genotypes with any evidence in a [[Pileup]].
   *
   * By "possible genotypes" we mean any genotype where both alleles in the genotype match the sequenced bases of at
   * least one element in the Pileup.
   *
   * See [[likelihoodsOfGenotypes()]] for argument descriptions.
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
    val genotypes = for {
      i <- 0 until alleles.size
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
   * @param elements the [[PileupElement]] instances across which the likelihoods are calculated.
   * @param genotypes the genotypes to calculate likelihoods for.
   * @param probabilityCorrect a function of [[PileupElement]] that gives the probability that the bases sequenced are
   *                           correct. See [[probabilityCorrectIgnoringAlignment()]] and
   *                           [[probabilityCorrectIncludingAlignment()]] for two reasonable functions to use here.
   *                           Note that the return value of this function is *always* expected to be a plain
   *                           probability, never a log prob, no matter if the computation is being done in log space or
   *                           not.
   * @param prior a function on genotypes that gives the prior probability that genotype is correct. This function should
   *             return a plain probability (NOT a log prob), even if the computation is being done in log space.
   * @param logSpace if true, the calculation is performed in log space and the probabilities are returned as log probs.
   * @param normalize if true, the probabilities returned are normalized so they sum to 1.
   * @return A sequence of probabilities corresponding to each genotype in the genotypes argument
   */
  def likelihoodsOfGenotypes(
    elements: Seq[PileupElement],
    genotypes: Seq[Genotype],
    probabilityCorrect: PileupElement => Double = probabilityCorrectIgnoringAlignment,
    prior: Genotype => Double = uniformPrior,
    logSpace: Boolean = false,
    normalize: Boolean = false): Seq[Double] = {

    val alleles = genotypes.flatMap(_.alleles).distinct.toIndexedSeq.sorted // the distinct alleles in our genotypes
    val alleleToIndex = alleles.zipWithIndex.toMap // map from allele -> allele index in our alleles sequence.
    val numElements = elements.size
    val numAlleles = alleles.size

    // Create and populate the alleleElementProbabilities array.
    // This is logically a two dimensional array where the element at position
    //    (allele index, element index)
    // is:
    //    probability(allele, element).
    //
    // where the probability is defined as in the header comment.
    //
    // Since java doesn't have true two dimensional arrays, we fake it by manually indexing a 1 dimensional array.
    // This is actually detectably faster to allocate than an Array[Array[Double]].
    val alleleElementProbabilities = Array.ofDim[Double](numAlleles * numElements)
    var alleleIndex = 0
    while (alleleIndex < alleles.size) {
      var elementIndex = 0
      val allele = alleles(alleleIndex)
      while (elementIndex < elements.size) {
        val element = elements(elementIndex)
        val successProbability = probabilityCorrect(element)
        if (allele == element.allele) {
          alleleElementProbabilities(alleleIndex * numElements + elementIndex) = successProbability
        } else {
          alleleElementProbabilities(alleleIndex * numElements + elementIndex) = 1 - successProbability
        }
        elementIndex += 1
      }
      alleleIndex += 1
    }

    // Calculate likelihoods using our alleleElementProbabilities.
    val likelihoods = genotypes.map(genotype => {
      assume(genotype.alleles.size == 2, "Non-diploid genotype not supported")
      val alleleOffset1 = numElements * alleleToIndex(genotype.alleles(0))
      val alleleOffset2 = numElements * alleleToIndex(genotype.alleles(1))
      var result = 0.0
      if (logSpace) {
        var elementIndex = 0
        while (elementIndex < elements.length) {
          result += math.log(
            alleleElementProbabilities(alleleOffset1 + elementIndex)
              + alleleElementProbabilities(alleleOffset2 + elementIndex))
          elementIndex += 1
        }
        result += math.log(prior(genotype)) - math.log(genotype.ploidy) * numElements
      } else {
        result = 1.0
        var elementIndex = 0
        while (elementIndex < elements.length) {
          result *= (
            alleleElementProbabilities(alleleOffset1 + elementIndex)
            + alleleElementProbabilities(alleleOffset2 + elementIndex))
          elementIndex += 1
        }
        result = result * prior(genotype) / math.pow(genotype.ploidy, numElements)
      }
      result
    })

    // Normalize results if necessary.
    if (normalize) {
      if (logSpace) {
        val totalLikelihood = math.log(likelihoods.map(math.exp).sum)
        likelihoods.map(_ - totalLikelihood)
      } else {
        val totalLikelihood = likelihoods.sum
        likelihoods.map(_ / totalLikelihood)
      }
    } else {
      likelihoods
    }
  }
}
