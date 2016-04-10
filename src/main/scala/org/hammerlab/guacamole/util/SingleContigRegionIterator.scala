package org.hammerlab.guacamole.util

import org.hammerlab.guacamole.HasReferenceRegion

/**
  * Wraps an iterator of regions sorted by contig name. Implements an iterator that gives regions only for the specified
  * contig name, then stops.
  */
class SingleContigRegionIterator[Mapped <: HasReferenceRegion](contig: String,
                                                               iterator: BufferedIterator[Mapped])
  extends Iterator[Mapped] {

  def hasNext = iterator.hasNext && iterator.head.referenceContig == contig
  def next() = if (hasNext) iterator.next() else throw new NoSuchElementException
}

