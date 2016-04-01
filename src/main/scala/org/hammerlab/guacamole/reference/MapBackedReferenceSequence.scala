package org.hammerlab.guacamole.reference

import org.hammerlab.guacamole.Bases

import scala.collection.mutable

/**
 * A ContigSequence implementation that uses a Map to store only a subset of bases. This is what you get if you load
 * a "partial fasta", which we do sometimes in tests.
 *
 * @param wrapped
 */
case class MapBackedReferenceSequence(length: Int, wrapped: mutable.LinkedHashMap[Int, Byte]) extends ContigSequence {
  def apply(index: Int): Byte = wrapped.getOrElse(index, Bases.N)

  override def iterator: Iterator[Byte] = {
    throw new NotImplementedError(
      s"Iterating over all bases in a sparse, test-only representation of a contig is undefined."
    )
  }

  override def equals(other: Any): Boolean = {
    other match {
      case o: MapBackedReferenceSequence ⇒ length == o.length && wrapped == o.wrapped
      case _                             ⇒ false
    }
  }

  override def slice(start: Int, end: Int): ContigSequence = (start until end).map(i ⇒ this(i))
}

object MapBackedReferenceSequence {
  def apply(length: Int, wrapped: Seq[(Int, Byte)]): MapBackedReferenceSequence = {
    new MapBackedReferenceSequence(length, mutable.LinkedHashMap(wrapped: _*))
  }
}
