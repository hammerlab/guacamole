package org.hammerlab.guacamole.readsets.iterator.overlaps

import org.hammerlab.guacamole.loci.iterator.SkippableLocusKeyedIterator
import org.hammerlab.guacamole.reference.{Interval, Locus}

import scala.collection.mutable

/**
 *  For each locus overlapped by @regions (± @halfWindowSize), emit the locus and the reads that overlap it (grouped
 *  into a {{LociIntervals}}).
 */
case class LociOverlapsIterator[I <: Interval](halfWindowSize: Int,
                                               regions: BufferedIterator[I])
  extends SkippableLocusKeyedIterator[Iterable[I]] {

  private val queue = new mutable.PriorityQueue[I]()(Interval.orderByEndDesc[I])

  override def _advance: Option[(Locus, Iterable[I])] = {
    updateQueue()

    if (queue.isEmpty) {
      if (!regions.hasNext) {
        return None
      }

      locus = regions.head.start - halfWindowSize
      return _advance
    }

    Some(locus -> queue)
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
