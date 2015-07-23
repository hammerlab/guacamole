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

package org.hammerlab.guacamole.pileup

import org.hammerlab.guacamole.Bases
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.variants.{ Allele, Genotype }

/**
 * A [[Pileup]] at a locus contains a sequence of [[PileupElement]] instances, one for every read that overlaps that
 * locus. Each [[PileupElement]] specifies the base read at the given locus in a particular read. It also keeps track
 * of the read itself and the offset of the base in the read.
 *
 * @param referenceName The contig name for all elements in this pileup.
 * @param locus The locus on the reference genome
 * @param referenceBase The reference base at [[locus]] on the reference genome
 * @param elements Sequence of [[PileupElement]] instances giving the sequenced bases that align to a particular
 *                 reference locus, in arbitrary order.
 */
case class Pileup(referenceName: String, locus: Long, referenceBase: Byte, elements: Seq[PileupElement]) {
  /** The first element in the pileup. */
  lazy val head = {
    assume(elements.nonEmpty, "Empty pileup")
    elements.head
  }

  assume(elements.forall(_.read.referenceContig == referenceName),
    "Pileup reference name '%s' does not match read reference name(s): %s".format(
      referenceName, elements.map(_.read.referenceContig).filter(_ != referenceName).mkString(",")))
  assume(elements.forall(_.locus == locus), "Reads in pileup have mismatching loci")

  lazy val distinctAlleles: Seq[Allele] = elements.map(_.allele).distinct.sorted.toIndexedSeq

  lazy val sampleName = elements.head.read.sampleName

  /**
   * Split this [[Pileup]] by sample name. Returns a map from sample name to [[Pileup]] instances that use only reads
   * from that sample.
   */
  lazy val bySample: Map[String, Pileup] = {
    elements.groupBy(element => Option(element.read.sampleName).map(_.toString).getOrElse("default")).map({
      case (sample, elems) => (sample, Pileup(referenceName, locus, referenceBase, elems))
    })
  }

  /**
   * Split this [[Pileup]] by the read token. Returns a map from token to [[Pileup]] instances that use only reads
   * with that token.
   */
  lazy val byToken: Map[Int, Pileup] = {
    elements.groupBy(element => element.read.token).map({
      case (token, elems) => (token, Pileup(referenceName, locus, referenceBase, elems))
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
   * @param newReferenceBase The reference base at the newLocus
   * @param newReads The *new* reads, i.e. those that overlap the new locus, but not the current locus.
   * @return A new [[Pileup]] at the given locus.
   */
  def atGreaterLocus(newLocus: Long, newReferenceBase: Byte, newReads: Iterator[MappedRead]) = {
    assume(elements.isEmpty || newLocus > locus,
      "New locus (%d) not greater than current locus (%d)".format(newLocus, locus))
    if (elements.isEmpty && newReads.isEmpty) {
      // Optimization for common case.
      // If there are no reads, we won't know what the reference base is
      Pileup(referenceName, newLocus, newReferenceBase, Vector.empty[PileupElement])
    } else {
      // This code gets called many times. We are using while loops for performance.
      val builder = Vector.newBuilder[PileupElement]
      builder.sizeHint(elements.size) // We expect to have about the same number of elements as we currently have.

      // Add current elements that overlap the new locus.
      val iterator = elements.iterator
      while (iterator.hasNext) {
        val element = iterator.next()
        if (element.read.overlapsLocus(newLocus)) {
          builder += element.advanceToLocus(newLocus, newReferenceBase)
        }
      }

      // Add elements for new reads.
      while (newReads.hasNext) {
        builder += PileupElement(newReads.next(), newLocus, newReferenceBase)
      }

      val newPileupElements = builder.result
      Pileup(referenceName, newLocus, newReferenceBase, newPileupElements)
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
   *
   * Find the reference base at [[locus]] in the reads at this pileup
   *
   * @param reads collection of reads that overlap locus
   * @param locus locus to find reference base at
   * @return First non N reference base at this locus
   */
  def referenceBaseAtLocus(reads: Seq[MappedRead], locus: Long): Byte = {
    // We take the reference base from the first read with a standard A, C, T, G base
    // TODO: Is there a case where the reads disagree
    val readWithStandardReferenceBase = reads.find(read => Bases.isStandardBase(read.getReferenceBaseAtLocus(locus)))
    readWithStandardReferenceBase match {
      case Some(read) => read.getReferenceBaseAtLocus(locus)
      case None       => Bases.N
    }
  }

  /**
   * Given reads and a locus, returns a [[Pileup]] at the specified locus.
   *
   * @param reads Sequence of reads, in any order, that may or may not overlap the locus.
   * @param locus The locus to return a [[Pileup]] at.
   * @param referenceBase The reference base at [[locus]]
   * @return A [[Pileup]] at the given locus.
   */
  def apply(reads: Seq[MappedRead], referenceName: String, locus: Long, referenceBase: Byte): Pileup = {
    //TODO: Is this call to overlaps locus necessary?
    val elements = reads.filter(_.overlapsLocus(locus)).map(PileupElement(_, locus, referenceBase))
    Pileup(referenceName, locus, referenceBase, elements)
  }

  def apply(reads: Seq[MappedRead], referenceName: String, locus: Long): Pileup = {
    val overlappingReads = reads.filter(_.overlapsLocus(locus))
    val referenceBase = referenceBaseAtLocus(overlappingReads, locus)
    val elements = overlappingReads.map(PileupElement(_, locus, referenceBase))
    Pileup(referenceName, locus, referenceBase, elements)
  }
}
