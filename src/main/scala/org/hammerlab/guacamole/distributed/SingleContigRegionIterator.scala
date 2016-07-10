package org.hammerlab.guacamole.distributed

import org.hammerlab.guacamole.reference.{Contig, ReferenceRegion}

/**
 * Wraps an iterator of regions sorted by contig name. Implements an iterator that gives regions only for the specified
 * contig name, then stops.
 */
class SingleContigRegionIterator[R <: ReferenceRegion](contig: Contig,
                                                       iterator: BufferedIterator[R])
  extends Iterator[R] {

  def hasNext = iterator.hasNext && iterator.head.contig == contig
  def next() = if (hasNext) iterator.next() else throw new NoSuchElementException
}

