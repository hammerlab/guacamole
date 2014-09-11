package org.bdgenomics.guacamole.pileup

import org.bdgenomics.guacamole.Bases

/**
 * The Alignment of a read at a particular locus specifies:
 *
 *  - the Cigar operator for this read and locus.
 *
 *  - the base(s) read at the corresponding offset in the read.
 *
 *  - the base quality scores of the bases read.
 */
private[pileup] sealed abstract class Alignment

case class Insertion(readBases: Seq[Byte], baseQualities: Seq[Byte], referenceBase: Byte) extends Alignment

case class Match(base: Byte, baseQuality: Byte) extends Alignment
case class Mismatch(base: Byte, baseQuality: Byte) extends Alignment
case object Clipped extends Alignment

/**
 * For now, only emit a Deletion at the position immediately preceding the run of deleted bases, i.e. the position we
 * would ultimately emit a variant at (by VCF convention).
 *
 * For reads at loci in the middle of a deletion, emit MidDeletion below.
 *
 * Deletion stores the reference bases of the entire deletion (pulled from MD tag), which are emitted with a deletion
 * variant.
 *
 * @param referenceBases
 */
case class Deletion(referenceBases: Seq[Byte]) extends Alignment {
  override def equals(other: Any): Boolean = other match {
    case Deletion(otherBases) =>
      referenceBases.sameElements(otherBases)
    case _ =>
      false
  }
  val readBases = referenceBases.headOption.toSeq
  override def toString: String = "Deletion(%s)".format(Bases.basesToString(referenceBases))
}
object Deletion {
  def apply(referenceBases: String): Deletion =
    Deletion(Bases.stringToBases(referenceBases))
}

/**
 * Signifies that at a particular locus, a read has bases deleted.
 */
case object MidDeletion extends Alignment
