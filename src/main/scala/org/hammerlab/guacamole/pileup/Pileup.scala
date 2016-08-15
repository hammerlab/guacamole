package org.hammerlab.guacamole.pileup

import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.reference.{ContigName, ContigSequence, Locus}
import org.hammerlab.guacamole.variants.Allele

/**
 * A [[Pileup]] at a locus contains a sequence of [[PileupElement]] instances, one for every read that overlaps that
 * locus. Each [[PileupElement]] specifies the base read at the given locus in a particular read. It also keeps track
 * of the read itself and the offset of the base in the read.
 *
 * @param contigName The contig name for all elements in this pileup.
 * @param locus The locus on the reference genome
 * @param contigSequence The reference for this contig
 * @param elements Sequence of [[PileupElement]] instances giving the sequenced bases that align to a particular
 *                 reference locus, in arbitrary order.
 */
case class Pileup(contigName: ContigName,
                  locus: Locus,
                  contigSequence: ContigSequence,
                  elements: Seq[PileupElement]) {

  val referenceBase: Byte = contigSequence(locus.toInt)

  assume(elements.forall(_.read.contigName == contigName),
    "Pileup reference name '%s' does not match read reference name(s): %s".format(
      contigName, elements.map(_.read.contigName).filter(_ != contigName).mkString(",")))
  assume(elements.forall(_.locus == locus), "Reads in pileup have mismatching loci")

  lazy val distinctAlleles: Seq[Allele] = elements.map(_.allele).distinct.sorted.toVector

  lazy val sampleName = elements.head.read.sampleName

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
  def atGreaterLocus(newLocus: Locus, newReads: Iterator[MappedRead]) = {
    assume(elements.isEmpty || newLocus > locus,
      "New locus (%d) not greater than current locus (%d)".format(newLocus, locus))
    if (elements.isEmpty && newReads.isEmpty) {
      // Optimization for common case.
      // If there are no reads, we won't know what the reference base is
      Pileup(contigName, newLocus, contigSequence, Vector.empty[PileupElement])
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
        builder += PileupElement(newReads.next(), newLocus, contigSequence)
      }

      val newPileupElements = builder.result
      Pileup(contigName, newLocus, contigSequence, newPileupElements)
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
   * @param reads Sequence of reads, in any order, that must overlap the locus.
   * @param contigName The contig these reads lie on.
   * @param locus The locus to return a [[Pileup]] at.
   * @param contigSequence The reference for this pileup's contig
   * @return A [[Pileup]] at the given locus.
   */
  def apply(reads: Seq[MappedRead],
            contigName: ContigName,
            locus: Locus,
            contigSequence: ContigSequence): Pileup = {
    val elements = reads.map(PileupElement(_, locus, contigSequence))
    Pileup(contigName, locus, contigSequence, elements.toIndexedSeq)
  }
}
