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

package org.bdgenomics.guacamole

import org.bdgenomics.adam.rich.DecadentRead
import net.sf.samtools.{ CigarElement, Cigar, CigarOperator, TextCigarCodec }

/**
 * A [[Pileup]] at a locus contains a sequence of [[Pileup.Element]] instances, one for every read that overlaps that
 * locus. Each [[Pileup.Element]] specifies the base read at the given locus in a particular read. It also keeps track
 * of the read itself and the offset of the base in the read.
 *
 *
 * @param locus The locus on the reference genome
 *
 * @param elements Sequence of [[Pileup.Element]] instances giving the sequenced bases that align to a particular
 *                 reference locus, in arbitrary order.
 */
case class Pileup(locus: Long, elements: Seq[Pileup.Element]) {
  /** The first element in the pileup. */
  lazy val head = {
    assume(!elements.isEmpty, "Empty pileup")
    elements.head
  }

  /** The contig name for all elements in this pileup. */
  lazy val referenceName: String = head.read.record.contig.contigName.toString

  assume(elements.forall(_.read.record.contig.contigName.toString == referenceName),
    "Reads in pileup have mismatching reference names")
  assume(elements.forall(_.locus == locus), "Reads in pileup have mismatching loci")

  /** The reference nucleotide base at this pileup's locus. */
  lazy val referenceBase: Char = {
    val mdTag = head.read.record.mdTag.get.getReference(head.read.record)
    mdTag.charAt((head.locus - head.read.record.start).toInt)
  }

  /**
   * Split this [[Pileup]] by sample name. Returns a map from sample name to [[Pileup]] instances that use only reads
   * from that sample.
   */
  lazy val bySample: Map[String, Pileup] = {
    elements.groupBy(element => Option(element.read.record.recordGroupSample).map(_.toString).getOrElse("default")).map({
      case (sample, elements) => (sample, Pileup(locus, elements))
    })
  }

  /**
   * Returns a new [[Pileup]] at a different locus on the same contig.
   *
   * To enable an efficient implementation, the new locus must be greater than the current locus.
   *
   * @param newLocus The locus to move forward to.
   * @param newReads The *new* reads, i.e. those that overlap the new locus, but not the current locus.
   * @return A new [[Pileup]] at the given locus.
   */
  def atGreaterLocus(newLocus: Long, newReads: Iterator[DecadentRead]) = {
    assume(elements.isEmpty || newLocus > locus,
      "New locus (%d) must be greater than current locus (%d)".format(newLocus, locus))
    val reusableElements = elements.filter(element => Common.overlapsLocus(element.read.record, newLocus))
    val updatedElements = reusableElements.map(_.elementAtGreaterLocus(newLocus))
    val newElements = newReads.map(Pileup.Element(_, newLocus))
    Pileup(newLocus, updatedElements ++ newElements)
  }

}
object Pileup {
  /**
   * Given an iterator over (locus, new reads) pairs, returns an iterator of [[Pileup]] instances at the given loci.
   *
   * @param locusAndReads Iterator of (locus, new reads) pairs, where "new reads" are those that overlap the current
   *                      locus, but not the previous locus.
   *
   * @return An iterator of [[Pileup]] instances at the given loci.
   */
  def pileupsAtLoci(locusAndReads: Iterator[(Long, Iterable[DecadentRead])]): Iterator[Pileup] = {

    val empty: Seq[Pileup.Element] = Seq()
    val initialEmpty = Pileup(0, empty)

    val iterator = locusAndReads.scanLeft(initialEmpty)((prevPileup: Pileup, pair) => {
      val (locus, newReads) = pair
      prevPileup.atGreaterLocus(locus, newReads.iterator)
    })
    iterator.next() // Discard first element, the initial empty pileup.
    iterator
  }

  /**
   * Given reads and a locus, returns a [[Pileup]] at the specified locus.
   *
   * @param reads Sequence of reads, in any order, that may or may not overlap the locus.
   * @param locus The locus to return a [[Pileup]] at.
   * @return A [[Pileup]] at the given locus.
   */
  def apply(reads: Seq[DecadentRead], locus: Long): Pileup = {
    val elements = reads.filter(read => Common.overlapsLocus(read.record, locus)).map(Element(_, locus))
    val pileup = Pileup(locus, elements)
    assert(pileup.locus == locus, "New pileup has locus %d but exepcted %d".format(pileup.locus, locus))
    pileup
  }

  /**
   * A [[Pileup.Element]] represents a particular nucleotide sequenced by a particular read at a particular
   * reference locus.
   *
   * @param read The read this [[Element]] is coming from.
   * @param locus The reference locus.
   * @param readPosition The offset into the sequence of bases in the read that this element corresponds to.
   * @param cigar The parsed cigar object of this read.
   * @param indexInCigarElements Which cigar element in the read this element belongs to.
   * @param indexWithinCigarElement The offset of this element within the current cigar element
   */
  case class Element(
      read: DecadentRead,
      locus: Long,
      readPosition: Long,
      cigar: Cigar,
      indexInCigarElements: Long,
      indexWithinCigarElement: Long) {

    assume(locus >= read.record.getStart)
    assume(locus < read.record.end.get)
    assume(read.record.mdTag.isDefined, "Record has no MDTag.")

    /**
     * The alignment of a read combines the underlying Cigar operator
     * (match/mismatch/deletion/insertion) with the characters which were used from the read.
     */
    abstract class Alignment
    case class Insertion(bases: String) extends Alignment
    case class Deletion() extends Alignment
    case class Match(base: Char) extends Alignment
    case class Mismatch(base: Char) extends Alignment

    lazy val alignment: Alignment = {
      val cigarElement: CigarElement = cigar.getCigarElement(indexInCigarElements.toInt)
      val cigarOperator = cigarElement.getOperator
      cigarOperator match {
        case CigarOperator.I =>
          val startPos: Int = readPosition.toInt
          val endPos: Int = startPos + cigarElement.getLength - indexWithinCigarElement.toInt
          val bases = read.record.getSequence.toString.subSequence(startPos, endPos).toString
          Insertion(bases)
        case CigarOperator.M | CigarOperator.EQ | CigarOperator.X =>
          val base: Char = read.record.getSequence.charAt(readPosition.toInt)
          if (read.record.mdTag.get.isMatch(locus)) { Match(base) }
          else { Mismatch(base) }
        case CigarOperator.D | CigarOperator.S | CigarOperator.N |
          CigarOperator.H => Deletion()
        case CigarOperator.P =>
          // `P` CIGAR-ops should have been ignored earlier: 
          // in `findNextCigarElement`
          throw new AssertionError("Cannot deal with CIGAR-operator 'P'")
      }
    }

    /* If you only care about what the CigarOperator was at this position, but not its
     * associated sequence, then you can just one of these state variables.
     */
    lazy val isInsertion = alignment match { case Insertion(_) => true; case _ => false }
    lazy val isDeletion = alignment match { case Deletion() => true; case _ => false }
    lazy val isMismatch = alignment match { case Mismatch(_) => true; case _ => false }
    lazy val isMatch = alignment match { case Match(_) => true; case _ => false }

    /**
     * The sequenced nucleotides at this element.
     *
     * If the current element is a deletion, then return the empty string. If it's
     * an insertion, then this will be a string of length >= 1: the contents of
     * the inserted sequence starting at the current locus. Otherwise, this is
     * a string of length 1.
     */
    lazy val sequenceRead: String = alignment match {
      case Deletion()       => ""
      case Match(base)      => base.toString
      case Mismatch(base)   => base.toString
      case Insertion(bases) => bases
    }

    lazy val singleBaseRead: Char = alignment match {
      case Match(base)                           => base
      case Mismatch(base)                        => base
      case Insertion(bases) if bases.length == 1 => bases.charAt(0)
      case other =>
        throw new AssertionError("Not a match, mismatch, or single nucleotide insertion: " + other.toString)
    }

    /**
     * Determine the read position, cigar element index, and offset into that cigar element for a given locus.
     *
     * @param newLocus The desired locus of the new [[Pileup.Element]]. It must be greater than the current locus, and
     *                 not past the end of the current read.
     *
     * @return A tuple of (read position, cigar element index, an offset into that cigar element)
     *
     */
    private def findNextCigarElement(newLocus: Long): (Long, Long, Long) = {
      var currReadPos = readPosition
      var currReferencePos = locus
      for (i <- indexInCigarElements until cigar.numCigarElements()) {
        val cigarElt = cigar.getCigarElement(i.toInt)
        val cigarOp = cigarElt.getOperator
        if (cigarOp != CigarOperator.P) {
          val cigarEltLen = cigarElt.getLength
          val currEltEnd = currReferencePos + cigarEltLen
          if (currEltEnd > newLocus) {
            val offset = newLocus - currReferencePos
            val finalReadPos = if (cigarOp.consumesReadBases) currReadPos + offset else currReadPos
            return (finalReadPos, i, offset)
          }
          if (cigarOp.consumesReadBases) { currReadPos += cigarEltLen }
          if (cigarOp.consumesReferenceBases) { currReferencePos += cigarEltLen }
        }
      }
      throw new RuntimeException(
        "Couldn't find cigar element for locus %d, cigar string only extends to %d".format(newLocus, currReferencePos))
    }

    /**
     * Returns a new [[Pileup.Element]] of the same read at a different locus.
     *
     * To enable an efficient implementation, newLocus must be greater than the current locus.
     *
     * @param newLocus The desired locus of the new [[Pileup.Element]]. It must be greater than the current locus, and
     *                 not past the end of the current read.
     *
     * @return A new [[Pileup.Element]] at the given locus.
     */
    def elementAtGreaterLocus(newLocus: Long): Element = {
      if (newLocus == locus) { return this }

      assume(newLocus > locus, "Can't rewind to locus %d from %d. Pileups only advance.".format(newLocus, locus))
      val readEndPos = read.record.end.get
      assume(newLocus < readEndPos, "This read stops at position %d. Can't advance to %d".format(readEndPos, newLocus))

      val (newReadPosition, newIndexInCigarElements, newIndexWithinCigarElement) = findNextCigarElement(newLocus)

      assert(newIndexInCigarElements < cigar.numCigarElements(),
        "Invalid cigar element index: %d".format(newIndexInCigarElements))

      Element(
        read,
        newLocus,
        newReadPosition,
        cigar,
        newIndexInCigarElements,
        newIndexWithinCigarElement)
    }
  }
  object Element {
    /**
     * Create a new [[Pileup.Element]] backed by the given read at the specified locus. The read must overlap the locus.
     *
     */
    def apply(read: DecadentRead, locus: Long): Element = {
      assume(read.isAligned)
      assume(locus >= read.record.start)
      assume(read.record.end.isDefined)
      assume(locus < read.record.end.get)

      val cigar = TextCigarCodec.getSingleton.decode(read.record.getCigar.toString)
      val startElement = Element(
        read = read,
        locus = read.record.getStart,
        readPosition = 0,
        cigar = cigar,
        indexInCigarElements = 0,
        indexWithinCigarElement = 0)
      startElement.elementAtGreaterLocus(locus)
    }
  }
}

