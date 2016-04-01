package org.hammerlab.guacamole.reference

import org.hammerlab.guacamole.Bases

/**
 * A ContigSequence implementation that uses a Map to store only a subset of bases. This is what you get if you load
 * a "partial fasta".
 *
 * @param wrapped
 */
case class MapBackedReferenceSequence(length: Int, wrapped: Map[Int, Byte]) extends ContigSequence {
  def apply(index: Int): Byte = wrapped.getOrElse(index, Bases.N)

  override def iterator: Iterator[Byte] = {
    wrapped.valuesIterator
  }

  override def slice(start: Int, end: Int): ContigSequence = (start until end).map(i ⇒ this(i))
}
