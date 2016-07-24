package org.hammerlab.guacamole.readsets.iterator

import org.hammerlab.guacamole.loci.set.{LociIterator, LociSet}
import org.hammerlab.guacamole.reference.{ContigIterator, ReferenceRegion}
import org.hammerlab.magic.iterator.SimpleBufferedIterator

/**
 * Iterate over regions and loci, one contig at a time.
 *
 * Given an input iterator of regions and a set of loci, emit pairs of iterators that are contig-restricted views over
 * the inputs.
 *
 * Narrows a simultaneous iteration over [regions and loci] into per-contig chunks.
 */
class ContigsIterator[R <: ReferenceRegion](it: BufferedIterator[R], loci: LociSet)
  extends SimpleBufferedIterator[(ContigIterator[R], LociIterator)] {

  var contigRegions: ContigIterator[R] = _

  override def _advance: Option[(ContigIterator[R], LociIterator)] = {
    if (contigRegions != null) {
      while (contigRegions.hasNext) {
        contigRegions.next()
      }
    }
    contigRegions = null

    if (!it.hasNext)
      None
    else {
      contigRegions = ContigIterator(it)
      val lociContig = loci.onContig(contigRegions.contigName).iterator
      Some((contigRegions, lociContig))
    }
  }
}
