package org.hammerlab.guacamole.readsets.iterator.overlaps

import org.hammerlab.guacamole.loci.iterator.SkippableLociIterator
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.{ContigIterator, ReferenceRegion}

/**
 * For loci in @loci (and/or @forceCallLoci, as appropriate), emit reads that overlap within @halfWindowSize.
 */
class PositionRegionsIterator[R <: ReferenceRegion](halfWindowSize: Int,
                                                    loci: LociSet,
                                                    forceCallLoci: LociSet,
                                                    regions: BufferedIterator[R])
  extends PositionRegionsIteratorBase[R, LociIntervals[R], Iterable[R]](
    loci,
    forceCallLoci,
    regions,
    LociIntervals(_, Nil)
  ) {

  override def newObjIterator(contigRegions: ContigIterator[R]): SkippableLociIterator[LociIntervals[R]] =
    new LociOverlapsIterator(halfWindowSize, contigRegions)

  override def unwrapResult(t: LociIntervals[R]): Iterable[R] = t.intervals
}
