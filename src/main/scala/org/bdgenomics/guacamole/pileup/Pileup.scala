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

package org.bdgenomics.guacamole.pileup

import org.bdgenomics.guacamole.reads.MappedRead
import org.bdgenomics.guacamole.variants.{ Allele, Genotype }

/**
 * A [[Pileup]] at a locus contains a sequence of [[PileupElement]] instances, one for every read that overlaps that
 * locus. Each [[PileupElement]] specifies the base read at the given locus in a particular read. It also keeps track
 * of the read itself and the offset of the base in the read.
 *
 * @param locus The locus on the reference genome
 *
 * @param elements Sequence of [[PileupElement]] instances giving the sequenced bases that align to a particular
 *                 reference locus, in arbitrary order.
 */
case class Pileup(locus: Long, elements: Seq[PileupElement]) {
  /** The first element in the pileup. */
  lazy val head = {
    assume(elements.nonEmpty, "Empty pileup")
    elements.head
  }

  /** The contig name for all elements in this pileup. */
  lazy val referenceName: String = head.read.referenceContig

  assume(elements.forall(_.read.referenceContig == referenceName),
    "Reads in pileup have mismatching reference names")
  assume(elements.forall(_.locus == locus), "Reads in pileup have mismatching loci")

  /**
   * The reference nucleotide base at this pileup's locus.
   *
   * TODO(ryan): this should possibly be passed in to the [[Pileup]] constructor; each [[PileupElement]] can have
   * different notions of what the reference is (e.g. in the case of different-length deletions). Pulling the first idx
   * from the first [[PileupElement]] feels somewhat hacky.
   */
  lazy val referenceBase: Byte = head.referenceBase

  private[pileup] lazy val possibleAlleles = elements.map(_.allele).distinct.sorted

  /**
   * Generate possible genotypes from a pileup
   * Possible genotypes are all unique n-tuples of alleles that appear in the pileup.
   *
   * @return Sequence of possible genotypes for the pileup
   */
  lazy val possibleGenotypes: Seq[Genotype] = {
    for {
      i <- 0 until possibleAlleles.size
      j <- i until possibleAlleles.size
    } yield Genotype(possibleAlleles(i), possibleAlleles(j))
  }

  /**
   * For each possible genotype based on the pileup sequencedBases, compute the likelihood.
   *
   * @return Sequence of (Genotype, Likelihood)
   */
  def computeLikelihoods(prior: Genotype => Double = computeUniformGenotypePrior,
                         includeAlignmentLikelihood: Boolean = true,
                         normalize: Boolean = false): Seq[(Genotype, Double)] = {
    val genotypeLikelihoods = possibleGenotypes.map(_.likelihoodOfReads(elements, includeAlignmentLikelihood))

    if (normalize) {
      normalizeLikelihoods(possibleGenotypes.zip(genotypeLikelihoods))
    } else {
      possibleGenotypes.zip(genotypeLikelihoods)
    }
  }

  /**
   * See computeLikelihoods, same computation in log-space
   *
   * @return Sequence of (Genotype, LogLikelihood)
   */
  def computeLogLikelihoods(prior: Genotype => Double = computeUniformGenotypeLogPrior,
                            includeAlignmentLikelihood: Boolean = false): Seq[(Genotype, Double)] = {
    possibleGenotypes.map(g =>
      (g, prior(g) + g.logLikelihoodOfReads(elements, includeAlignmentLikelihood))
    )
  }

  /**
   * Compute prior probability for given genotype, in log-space
   *
   * @return 0.0 (default uniform prior)
   */
  protected def computeUniformGenotypeLogPrior(genotype: Genotype): Double = 0.0

  /**
   * Compute prior probability for given genotype
   *
   * @return 1.0 (default uniform prior)
   */
  protected def computeUniformGenotypePrior(genotype: Genotype): Double = 1.0

  /*
   * Helper function to normalize probabilities
   */
  def normalizeLikelihoods(likelihoods: Seq[(Genotype, Double)]): Seq[(Genotype, Double)] = {
    val totalLikelihood = likelihoods.map(_._2).sum
    likelihoods.map(genotypeLikelihood => (genotypeLikelihood._1, genotypeLikelihood._2 / totalLikelihood))
  }

  lazy val sampleName = elements.head.read.sampleName

  /**
   * Split this [[Pileup]] by sample name. Returns a map from sample name to [[Pileup]] instances that use only reads
   * from that sample.
   */
  lazy val bySample: Map[String, Pileup] = {
    elements.groupBy(element => Option(element.read.sampleName).map(_.toString).getOrElse("default")).map({
      case (sample, elems) => (sample, Pileup(locus, elems))
    })
  }

  /**
   * Split this [[Pileup]] by the read token. Returns a map from token to [[Pileup]] instances that use only reads
   * with that token.
   */
  lazy val byToken: Map[Int, Pileup] = {
    elements.groupBy(element => element.read.token).map({
      case (token, elems) => (token, Pileup(locus, elems))
    })
  }

  /**
   * Depth of pileup - number of reads at locus
   */
  lazy val depth: Int = elements.length

  /**
   * Number of positively stranded reads
   */
  lazy val positiveDepth: Int = elements.count(_.read.isPositiveStrand)

  /**
   * PileupElements that match the reference base
   */
  lazy val referenceElements = elements.filter(_.isMatch)

  /**
   * PileupElements that match the reference base
   */
  lazy val referenceDepth = referenceElements.size

  /**
   * Returns a new [[Pileup]] at a different locus on the same contig.
   *
   * To enable an efficient implementation, the new locus must be greater than the current locus.
   *
   * @param newLocus The locus to move forward to.
   * @param newReads The *new* reads, i.e. those that overlap the new locus, but not the current locus.
   * @return A new [[Pileup]] at the given locus.
   */
  def atGreaterLocus(newLocus: Long, newReads: Iterator[MappedRead]) = {
    assume(elements.isEmpty || newLocus > locus,
      "New locus (%d) not greater than current locus (%d)".format(newLocus, locus))
    if (elements.isEmpty && newReads.isEmpty) {
      // Optimization for common case.
      Pileup(newLocus, Vector.empty[PileupElement])
    } else {
      val builder = Vector.newBuilder[PileupElement]
      // We expect the next locus to have about the same number of elements as the current locus.
      builder.sizeHint(elements)

      // Add current elements that overlap the new locus.
      elements.foreach(element => {
        if (element.read.overlapsLocus(newLocus)) {
          builder += element.advanceToLocus(newLocus)
        }
      })

      // Add elements for new reads.
      builder ++= newReads.map(PileupElement(_, newLocus))
      Pileup(newLocus, builder.result)
    }
  }

  /**
   * Compute depth and positive strand depth of a particular alternate base
   * @param allele allele to consider
   * @return tuple of total depth and forward strand depth
   */
  def alleleReadDepthAndPositiveDepth(allele: Allele): (Int, Int) = {

    val alleleElements = elements.view.filter(_.allele == allele)
    val numAllelePositiveElements = alleleElements.count(_.read.isPositiveStrand)

    (alleleElements.size, numAllelePositiveElements)
  }
}

object Pileup {
  /**
   * Given reads and a locus, returns a [[Pileup]] at the specified locus.
   *
   * @param reads Sequence of reads, in any order, that may or may not overlap the locus.
   * @param locus The locus to return a [[Pileup]] at.
   * @return A [[Pileup]] at the given locus.
   */
  def apply(reads: Seq[MappedRead], locus: Long): Pileup = {
    val elements = reads.view.filter(_.overlapsLocus(locus)).map(PileupElement(_, locus)).toVector
    Pileup(locus, elements)
  }
}

