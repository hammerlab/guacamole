package org.hammerlab.guacamole.readsets.iterator.overlaps

import org.hammerlab.guacamole.loci.iterator.SkippableLociIterator
import org.hammerlab.guacamole.reference.Position.Locus
import org.hammerlab.guacamole.reference.{HasLocus, Interval}

import scala.collection.mutable

// Simple class joining a Locus to some Intervals.
case class LociIntervals[+I <: Interval](locus: Locus, intervals: Iterable[I]) extends HasLocus

/**
 *  For each locus overlapped by @regions (± @halfWindowSize), emit the locus and the reads that overlap it (grouped
 *  into a {{LociIntervals}}).
 */
class LociOverlapsIterator[I <: Interval](halfWindowSize: Int, regions: BufferedIterator[I])
  extends SkippableLociIterator[LociIntervals[I]] {

  private val queue = new mutable.PriorityQueue[I]()(Interval.orderByEndDesc[I])

  override def _advance: Option[LociIntervals[I]] = {
    updateQueue()

    if (queue.isEmpty) {
      if (!regions.hasNext) {
        return None
      }

      locus = regions.head.start - halfWindowSize
      return _advance
    }

    Some(LociIntervals(locus, queue))
  }

  private def updateQueue(): Unit = {
    val lowerLimit = locus - halfWindowSize
    val upperLimit = locus + halfWindowSize

    while (queue.headOption.exists(_.end <= lowerLimit)) {
      queue.dequeue
    }

    while (regions.hasNext && regions.head.start <= upperLimit) {
      if (regions.head.end > lowerLimit) {
        queue.enqueue(regions.head)
      }
      regions.next()
    }
  }

}
