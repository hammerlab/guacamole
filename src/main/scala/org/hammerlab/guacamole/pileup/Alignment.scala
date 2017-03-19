package org.hammerlab.guacamole.pileup

import org.hammerlab.genomics.bases.{ Base, Bases }

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
  def sequencedBases: Bases = Bases()
  def referenceBases: Bases = Bases()

  override def toString: String =
    "%s(%s,%s)".format(
      getClass.getSimpleName,
      referenceBases,
      sequencedBases
    )
}

case class Insertion(override val sequencedBases: Bases,
                     baseQualities: Seq[Byte])
  extends Alignment {
  override val referenceBases = Bases(sequencedBases.headOption.toVector)
}

class MatchOrMisMatch(val base: Base,
                      val baseQuality: Byte,
                      val referenceBase: Base)
  extends Alignment {
  override val sequencedBases = Bases(base)
  override val referenceBases = Bases(referenceBase)
}
object MatchOrMisMatch {
  def unapply(m: MatchOrMisMatch): Option[(Base, Byte)] = Some((m.base, m.baseQuality))
}

case class Match(override val base: Base,
                 override val baseQuality: Byte)
  extends MatchOrMisMatch(base, baseQuality, base)

case class Mismatch(override val base: Base,
                    override val baseQuality: Byte,
                    override val referenceBase: Base)
  extends MatchOrMisMatch(base, baseQuality, referenceBase)

/**
 * For now, only emit a Deletion at the position immediately preceding the run of deleted bases, i.e. the position we
 * would ultimately emit a variant at (by VCF convention).
 *
 * For reads at loci in the middle of a deletion, emit MidDeletion below.
 *
 * Deletion stores the reference bases of the entire deletion, including the base immediately before the deletion.
 */
case class Deletion(override val referenceBases: Bases,
                    baseQuality: Byte)
  extends Alignment {
  override def equals(other: Any): Boolean =
    other match {
      case Deletion(otherBases, _) ⇒
        referenceBases == otherBases
      case _ ⇒
        false
    }

  override val sequencedBases = Bases(referenceBases.headOption.toVector)
}

/**
 * Signifies that at a particular locus, a read has bases deleted.
 */
case class MidDeletion(referenceBase: Base)
  extends Alignment {
  override val referenceBases = Bases(referenceBase)
  override val sequencedBases = Bases()
}

case object Clipped extends Alignment

