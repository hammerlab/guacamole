package org.hammerlab.guacamole.readsets.iterator

import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.Coverage.PositionCoverage
import org.hammerlab.guacamole.loci.set.LociIterator
import org.hammerlab.guacamole.reference.{ContigIterator, ContigName, Interval, Position, ReferenceRegion}

import scala.collection.mutable

class ContigCoverageIterator private(halfWindowSize: Int,
                                     contig: ContigName,
                                     regions: BufferedIterator[Interval],
                                     loci: LociIterator)
  extends Iterator[PositionCoverage] {

  private val ends = mutable.PriorityQueue[Long]()(implicitly[Ordering[Long]].reverse)

  private var curPos = -1L
  private var _next: Coverage = _

  private def advance(): Boolean = {
    if (!loci.hasNext) {
      return false
    } else {
      assert(loci.hasNext)
      val nextLocus = loci.head
      if (ends.isEmpty) {
        if (regions.isEmpty)
          return false

        val nextReadWindowStart = regions.head.start - halfWindowSize
        if (nextReadWindowStart > nextLocus) {
          curPos = nextReadWindowStart
          loci.skipTo(nextReadWindowStart)
          return advance()
        }
      }
      curPos = loci.next()
    }

    val lowerLimit = math.max(0, curPos - halfWindowSize)
    val upperLimit = curPos + halfWindowSize

    var numDropped = 0
    while (ends.headOption.exists(_ <= lowerLimit)) {
      ends.dequeue()
      numDropped += 1
    }

    var numAdded = 0
    while (regions.nonEmpty && regions.head.start <= upperLimit) {
      ends.enqueue(regions.next().end)
      numAdded += 1
    }

    _next = Coverage(ends.size, numAdded, numDropped)
    true
  }

  override def hasNext: Boolean = {
    _next != null || advance()
  }

  override def next(): PositionCoverage = {
    val r =
      if (_next == null) {
        if (!advance()) throw new NoSuchElementException
        _next
      } else
        _next

    _next = null

    Position(contig, curPos) -> r
  }
}

object ContigCoverageIterator {
  def apply(halfWindowSize: Int,
            regions: ContigIterator[ReferenceRegion],
            loci: LociIterator): ContigCoverageIterator =
    new ContigCoverageIterator(halfWindowSize, regions.contigName, regions.buffered, loci)
}
