package org.hammerlab.guacamole.reference

/**
 * Given some ReferenceRegions, iterate over them as long as they are on @contig.
 *
 * Intended for repeated use on the same input-region-iterator, to segment it into runs of reads from the same contig.
 */
case class ContigIterator[+R <: ReferenceRegion](contigName: ContigName, regions: BufferedIterator[R])
  extends BufferedIterator[R] {

  override def head: R =
    if (hasNext)
      regions.head
    else
      throw new NoSuchElementException

  override def hasNext: Boolean = {
    regions.hasNext && regions.head.contigName == contigName
  }

  override def next(): R = {
    val n = regions.head
    regions.next()
    n
  }
}

object ContigIterator {
  def apply[R <: ReferenceRegion](regions: BufferedIterator[R]): ContigIterator[R] = {
    ContigIterator(regions.head.contigName, regions)
  }
}

