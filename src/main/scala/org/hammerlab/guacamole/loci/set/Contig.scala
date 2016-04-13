package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.loci.SimpleRange
import org.hammerlab.guacamole.loci.map.{Contig => LociMapContig}

/**
 * A set of loci on a single contig.
 */
case class Contig(map: LociMapContig[Long]) {

  /** Is the given locus contained in this set? */
  def contains(locus: Long): Boolean = map.contains(locus)

  /** Returns a sequence of ranges giving the intervals of this set. */
  def ranges(): Iterable[SimpleRange] = map.ranges

  /** Number of loci in this set. */
  def count(): Long = map.count

  /** Is this set empty? */
  def isEmpty: Boolean = map.isEmpty

  /** Iterator through loci in this set, sorted. */
  def iterator(): ContigIterator = new ContigIterator(this)

  /** Returns the union of this set with another. Both must be on the same contig. */
  def union(other: Contig): Contig = Contig(map.union(other.map))

  /** Returns whether a given genomic region overlaps with any loci in this LociSet. */
  def intersects(start: Long, end: Long) = map.getAll(start, end).nonEmpty

  override def toString: String = truncatedString(Int.MaxValue)

  /** String representation, truncated to maxLength characters. */
  def truncatedString(maxLength: Int = 100): String = map.truncatedString(maxLength, includeValues = false)
}
