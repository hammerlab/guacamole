package org.hammerlab.guacamole.readsets.iterator.overlaps

import org.hammerlab.guacamole.loci.iterator.SkippableLocusKeyedIterator
import org.hammerlab.guacamole.reads.HasSampleId
import org.hammerlab.guacamole.readsets.PerSample
import org.hammerlab.guacamole.readsets.iterator.overlaps.LociIntervalsPerSample.SampleInterval
import org.hammerlab.guacamole.reference.{Interval, Locus, ReferenceRegion}

import scala.collection.mutable

/**
 *  For each locus overlapped by @regions (± @halfWindowSize), emit the locus and the regions that overlap it (grouped
 *  by sample ID).
 */
case class LociOverlapsPerSampleIterator[I <: SampleInterval](numSamples: Int,
                                                              halfWindowSize: Int,
                                                              regions: BufferedIterator[I])
  extends SkippableLocusKeyedIterator[PerSample[Iterable[I]]] {

  private val queues =
    Vector.fill(
      numSamples
    )(
      new mutable.PriorityQueue[I]()(Interval.orderByEndDesc[I])
    )

  override def _advance: Option[(Locus, PerSample[Iterable[I]])] = {
    updateQueue()

    if (queues.forall(_.isEmpty)) {
      if (!regions.hasNext)
        return None

      locus = regions.head.start - halfWindowSize
      return _advance
    }

    Some(locus -> queues)
  }

  private def updateQueue(): Unit = {
    val lowerLimit = locus - halfWindowSize
    val upperLimit = locus + halfWindowSize

    for { queue <- queues } {
      while (queue.headOption.exists(_.end <= lowerLimit)) {
        queue.dequeue
      }
    }

    while (regions.hasNext && regions.head.start <= upperLimit) {
      if (regions.head.end > lowerLimit) {
        queues(regions.head.sampleId).enqueue(regions.head)
      }
      regions.next()
    }
  }
}

object LociIntervalsPerSample {
  type SampleInterval = Interval with HasSampleId
  type SampleRegion = ReferenceRegion with HasSampleId
}
