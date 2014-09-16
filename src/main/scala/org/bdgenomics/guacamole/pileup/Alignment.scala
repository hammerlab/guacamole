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
private[pileup] sealed abstract class Alignment {
  def sequencedBases: Seq[Byte] = Seq[Byte]()
  def referenceBases: Seq[Byte] = Seq[Byte]()
}

case class Insertion(override val sequencedBases: Seq[Byte], baseQualities: Seq[Byte]) extends Alignment {
  override val referenceBases: Seq[Byte] = sequencedBases.headOption.toSeq
}

class MatchOrMisMatch(val base: Byte, val baseQuality: Byte, val referenceBase: Byte) extends Alignment {
  override val sequencedBases: Seq[Byte] = Seq(base)
  override val referenceBases: Seq[Byte] = Seq(referenceBase)
}
object MatchOrMisMatch {
  def unapply(m: MatchOrMisMatch): Option[(Byte, Byte)] = Some((m.base, m.baseQuality))
}

case class Match(override val base: Byte, override val baseQuality: Byte)
  extends MatchOrMisMatch(base, baseQuality, base)
case class Mismatch(override val base: Byte, override val baseQuality: Byte, override val referenceBase: Byte)
  extends MatchOrMisMatch(base, baseQuality, referenceBase)

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
case class Deletion(override val referenceBases: Seq[Byte]) extends Alignment {
  override def equals(other: Any): Boolean = other match {
    case Deletion(otherBases) =>
      referenceBases.sameElements(otherBases)
    case _ =>
      false
  }
  override val sequencedBases = referenceBases.headOption.toSeq
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
case object Clipped extends Alignment

