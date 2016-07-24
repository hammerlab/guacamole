package org.hammerlab.guacamole.readsets.iterator

import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.iterator.SkippableLocusKeyedIterator
import org.hammerlab.guacamole.reference.{ContigIterator, Locus, ReferenceRegion}

import scala.collection.mutable

case class ContigCoverageIterator(halfWindowSize: Int,
                                  regions: ContigIterator[ReferenceRegion])
  extends SkippableLocusKeyedIterator[Coverage] {

  private val ends = mutable.PriorityQueue[Long]()(implicitly[Ordering[Long]].reverse)

  override def _advance: Option[(Locus, Coverage)] = {
    var lowerLimit = math.max(0, locus - halfWindowSize)
    var upperLimit = locus + halfWindowSize

    while (ends.headOption.exists(_ <= lowerLimit)) {
      ends.dequeue()
    }

    if (ends.isEmpty) {
      if (regions.isEmpty)
        return None

      val nextReadWindowStart = regions.head.start - halfWindowSize
      if (nextReadWindowStart > locus) {
        skipTo(nextReadWindowStart)
        upperLimit = locus + halfWindowSize
      }
    }

    var numAdded = 0
    while (regions.nonEmpty && regions.head.start <= upperLimit) {
      ends.enqueue(regions.next().end)
      numAdded += 1
    }

    Some(locus -> Coverage(ends.size, numAdded))
  }
}
