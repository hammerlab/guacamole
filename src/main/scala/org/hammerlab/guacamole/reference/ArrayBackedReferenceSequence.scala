package org.hammerlab.guacamole.reference

import java.util.NoSuchElementException

import org.apache.spark.broadcast.Broadcast
import org.hammerlab.genomics.bases.{ Base, Bases }
import org.hammerlab.genomics.reference.{ ContigName, ContigSequence, Locus, NumLoci }

/**
 * The standard ContigSequence implementation, which is an Array of bases.
 *
 * TODO: Arrays can't be more than 2³¹ long, use 2bit instead?
 */
case class ArrayBackedReferenceSequence(contigName: ContigName,
                                        wrapped: Broadcast[Array[Byte]])
  extends ContigSequence {

  /**
   * This is basically a hack that bakes in the clumsy injecting of [[Locus]] into [[Array]]-indexes ([[Int]]s).
   * A more robust implementation might add a chunking layer with multiple [[Array]]s able to seamlessly handle contigs
   * beyond 2³¹ bases in length.
   */
  implicit def locusToInt(locus: Locus): Int = locus.locus.toInt

  val length: NumLoci = NumLoci(wrapped.value.length)

  override def apply(locus: Locus): Base =
    try {
      wrapped.value(locus)
    } catch {
      case e: NoSuchElementException ⇒
        throw new Exception(s"Position $contigName:$locus missing from reference", e)
    }

  override def slice(start: Locus, length: Int): Bases = {
    val end = start + length
    if (start < Locus(0) || NumLoci(end) > this.length)
      throw new Exception(
        s"Illegal reference slice: $contigName:[$start,$end) (valid range: [0,$length)"
      )
    else
      wrapped.value.slice(start, end).map[Base, Bases](b ⇒ b: Base)
  }
}
