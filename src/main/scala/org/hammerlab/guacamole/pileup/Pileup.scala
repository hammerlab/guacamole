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

import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.reference.ContigSequence
import org.hammerlab.guacamole.variants.Allele

/**
 * A [[Pileup]] at a locus contains a sequence of [[PileupElement]] instances, one for every read that overlaps that
 * locus. Each [[PileupElement]] specifies the base read at the given locus in a particular read. It also keeps track
 * of the read itself and the offset of the base in the read.
 *
 * @param contig The contig name for all elements in this pileup.
 * @param locus The locus on the reference genome
 * @param reference The reference for this contig
 * @param elements Sequence of [[PileupElement]] instances giving the sequenced bases that align to a particular
 *                 reference locus, in arbitrary order.
 */
case class Pileup(contig: String,
                  locus: Long,
                  reference: ContigSequence,
                  elements: Seq[PileupElement]) {

  val referenceBase: Byte = reference(locus.toInt)

  /** The first element in the pileup. */
  lazy val head = {
    assume(elements.nonEmpty, "Empty pileup")
    elements.head
  }

  assume(elements.forall(_.read.contig == contig),
    "Pileup reference name '%s' does not match read reference name(s): %s".format(
      contig, elements.map(_.read.contig).filter(_ != contig).mkString(",")))
  assume(elements.forall(_.locus == locus), "Reads in pileup have mismatching loci")

  lazy val distinctAlleles: Seq[Allele] = elements.map(_.allele).distinct.sorted.toVector

  lazy val sampleName = elements.head.read.sampleName

  /**
   * Split this [[Pileup]] by sample name. Returns a map from sample name to [[Pileup]] instances that use only reads
   * from that sample.
   */
  lazy val bySample: Map[String, Pileup] = {
    elements.groupBy(element => Option(element.read.sampleName).map(_.toString).getOrElse("default")).map({
      case (sample, newElements) => (sample, Pileup(contig, locus, reference, newElements))
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
      // If there are no reads, we won't know what the reference base is
      Pileup(contig, newLocus, reference, Vector.empty[PileupElement])
    } else {
      // This code gets called many times. We are using while loops for performance.
      val builder = Vector.newBuilder[PileupElement]
      builder.sizeHint(elements.size) // We expect to have about the same number of elements as we currently have.

      // Add current elements that overlap the new locus.
      val iterator = elements.iterator
      while (iterator.hasNext) {
        val element = iterator.next()
        if (element.read.overlapsLocus(newLocus)) {
          builder += element.advanceToLocus(newLocus)
        }
      }

      // Add elements for new reads.
      while (newReads.hasNext) {
        builder += PileupElement(newReads.next(), newLocus, reference)
      }

      val newPileupElements = builder.result
      Pileup(contig, newLocus, reference, newPileupElements)
    }
  }

  /**
   * Compute depth and positive strand depth of a particular alternate base
   *
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
   * @param reference The reference for this pileup's contig
   * @return A [[Pileup]] at the given locus.
   */
  def apply(reads: Seq[MappedRead], contig: String, locus: Long, reference: ContigSequence): Pileup = {
    //TODO: Is this call to overlaps locus necessary?
    val elements = reads.filter(_.overlapsLocus(locus)).map(PileupElement(_, locus, reference))
    Pileup(contig, locus, reference, elements.toIndexedSeq)
  }
}
