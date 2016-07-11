package org.hammerlab.guacamole.readsets.iterator.overlaps.per_sample

import org.hammerlab.guacamole.loci.iterator.SkippableLociIterator
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.readsets.PerSample
import org.hammerlab.guacamole.readsets.iterator.overlaps.PositionRegionsIteratorBase
import org.hammerlab.guacamole.reference.{ContigIterator, ReferenceRegion}

/**
 * For loci in @loci (and/or @forceCallLoci, as appropriate), emit reads that overlap within @halfWindowSize, grouped by
 * sampleId.
 */
class PositionRegionsPerSampleIterator[R <: ReferenceRegion with HasSampleId](halfWindowSize: Int,
                                                                              numSamples: Int,
                                                                              loci: LociSet,
                                                                              forceCallLoci: LociSet,
                                                                              regions: BufferedIterator[R])
  extends PositionRegionsIteratorBase[R, LociIntervalsPerSample[R], PerSample[Iterable[R]]](
    loci,
    forceCallLoci,
    regions,
    LociIntervalsPerSample(_, Vector.fill(numSamples)(Nil))
  ) {

  override def newObjIterator(contigRegions: ContigIterator[R]): SkippableLociIterator[LociIntervalsPerSample[R]] =
    new LociOverlapsPerSampleIterator(halfWindowSize, numSamples, contigRegions)

  override def unwrapResult(t: LociIntervalsPerSample[R]): PerSample[Iterable[R]] = t.intervals
}
