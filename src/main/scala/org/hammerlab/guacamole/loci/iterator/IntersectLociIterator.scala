package org.hammerlab.guacamole.loci.iterator

import org.hammerlab.guacamole.reference.HasLocus
import org.hammerlab.guacamole.reference.Position.Locus
import org.hammerlab.magic.iterator.SimpleBufferedIterator

/**
 * Merge two iterators of loci-keyed objects (the former just an Iterator[Locus]) that exist on the same contig.
 */
class IntersectLociIterator[+T <: HasLocus](loci: SkippableLociIterator[Locus],
                                            lociObjs: SkippableLociIterator[T])
  extends SimpleBufferedIterator[T] {

  override def _advance: Option[T] = {
    if (!loci.hasNext) return None
    if (!lociObjs.hasNext) return None

    val nextAllowedLocus = loci.head
    val obj = lociObjs.head
    val nextObjectLocus = obj.locus

    if (nextObjectLocus > nextAllowedLocus) {
      loci.skipTo(nextObjectLocus)
      _advance
    } else if (nextAllowedLocus > nextObjectLocus) {
      lociObjs.skipTo(nextAllowedLocus)
      _advance
    } else {
      loci.next()
      lociObjs.next()
      Some(obj)
    }
  }

}
