package org.hammerlab.guacamole.readsets.iterator.overlaps.per_sample

import org.hammerlab.guacamole.loci.iterator.SkippableLociIterator
import org.hammerlab.guacamole.readsets.iterator.overlaps.per_sample.LociIntervalsPerSample.SampleInterval
import org.hammerlab.guacamole.readsets.{PerSample, SampleId}
import org.hammerlab.guacamole.reference.Position.Locus
import org.hammerlab.guacamole.reference.{HasLocus, Interval}

import scala.collection.mutable

trait HasSampleId {
  def sampleId: SampleId
}

// Simple class grouping a Locus with some SampleIntervals (Intervals that also have a `sampleId` field).
case class LociIntervalsPerSample[I <: SampleInterval](locus: Locus,
                                                       intervals: PerSample[Iterable[I]])
  extends HasLocus

/**
 *  For each locus overlapped by @regions (± @halfWindowSize), emit the locus and the reads that overlap it (grouped
 *  into a {{LociIntervalsPerSample}}).
 */
class LociOverlapsPerSampleIterator[I <: SampleInterval](halfWindowSize: Int,
                                                         numSamples: Int,
                                                         regions: BufferedIterator[I])
  extends SkippableLociIterator[LociIntervalsPerSample[I]] {

  private val queues = Vector.fill(numSamples)(new mutable.PriorityQueue[I]()(Interval.orderByEndDesc[I]))

  override def _advance: Option[LociIntervalsPerSample[I]] = {
    updateQueue()

    if (queues.forall(_.isEmpty)) {
      if (!regions.hasNext)
        return None

      locus = regions.head.start - halfWindowSize
      return _advance
    }

    Some(LociIntervalsPerSample(locus, queues))
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
}
