package org.hammerlab.guacamole.pileup

import org.hammerlab.guacamole.util.Bases

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

  override def toString: String =
    "%s(%s,%s)".format(
      getClass.getSimpleName,
      Bases.basesToString(referenceBases),
      Bases.basesToString(sequencedBases)
    )
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
 * Deletion stores the reference bases of the entire deletion, including the base immediately before the deletion.
 *
 * @param referenceBases
 */
case class Deletion(override val referenceBases: Seq[Byte], baseQuality: Byte) extends Alignment {
  override def equals(other: Any): Boolean = other match {
    case Deletion(otherBases, _) =>
      referenceBases.sameElements(otherBases)
    case _ =>
      false
  }
  override val sequencedBases = referenceBases.headOption.toSeq
}
object Deletion {
  def apply(referenceBases: String, baseQuality: Byte): Deletion =
    Deletion(Bases.stringToBases(referenceBases), baseQuality)
}

/**
 * Signifies that at a particular locus, a read has bases deleted.
 */
case class MidDeletion(referenceBase: Byte) extends Alignment {
  override val referenceBases = Seq(referenceBase)
  override val sequencedBases = Seq.empty
}

case object Clipped extends Alignment

