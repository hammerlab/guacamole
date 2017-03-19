package org.hammerlab.guacamole.reference

import java.util.NoSuchElementException

import org.apache.spark.broadcast.Broadcast
import org.hammerlab.genomics.bases.{ Base, Bases }
import org.hammerlab.genomics.reference.{ ContigName, ContigSequence, Locus, NumLoci }

/**
 * A ContigSequence implementation that uses a Map to store only a subset of bases. This is what you get if you load
 * a "partial fasta". This is used in tests.
 */
case class MapBackedReferenceSequence(contigName: ContigName,
                                      length: NumLoci,
                                      wrapped: Broadcast[Map[Locus, Base]])
  extends ContigSequence {

  override def apply(locus: Locus): Base =
    try {
      wrapped.value(locus)
    } catch {
      case e: NoSuchElementException ⇒
        throw new Exception(s"Position $contigName:$locus missing from reference", e)
    }

  override def slice(start: Locus, length: Int): Bases =
    Bases(
      (0 until length)
        .map(idx ⇒ apply(start + idx))
        .toVector
    )
}
