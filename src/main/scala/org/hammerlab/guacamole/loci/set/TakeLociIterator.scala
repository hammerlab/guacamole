package org.hammerlab.guacamole.loci.set

import java.lang.{Long => JLong}

import com.google.common.collect.{Range => JRange}
import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.Coverage.PositionCoverage
import org.hammerlab.guacamole.reference.Position

import scala.collection.mutable.ArrayBuffer

class TakeLociIterator(it: BufferedIterator[PositionCoverage],
                       maxRegionsPerPartition: Int,
                       throwOnInvalidDepth: Boolean = false)
  extends BufferedIterator[LociSet] {

  var _next: Option[LociSet] = None

  override def hasNext: Boolean = {
    if (_next.isDefined) return true

    var curNumRegions = 0

    val contigs = ArrayBuffer[Contig]()

    var takingLoci = true

    // Outer loop iterates over contigs.
    while (it.hasNext && takingLoci) {

      var (Position(curContig, _), _) = it.head
      var continuingContig = true
      var start: Long = -1L
      var end: Long = -1L

      // Inner loop accumulates loci only within the current contig.
      while (it.hasNext && takingLoci && continuingContig) {
        val (Position(nextContig, nextLocus), Coverage(depth, starts, _)) = it.head
        if (curContig == nextContig) {

          // When starting a new range, we inherit not just the reads that start at our current locus, but all reads
          // that cover the window around the current locus.
          if (curNumRegions == 0)
            if (depth <= maxRegionsPerPartition) {
              curNumRegions += depth
              start = nextLocus
              end = nextLocus + 1
              it.next()
            } else if (throwOnInvalidDepth)
              throw new Exception(s"$curContig:$start: depth $depth exceeds maximum $maxRegionsPerPartition")
            else
              it.next()
          else if (curNumRegions + starts <= maxRegionsPerPartition) {
            curNumRegions += starts
            if (start < 0) start = nextLocus
            end = nextLocus + 1
            it.next()
          } else
            takingLoci = false

        } else
          continuingContig = false
      }

      if (start >= 0 && end >= 0) {
        val contig = Contig(curContig, List(JRange.closedOpen(start: JLong, end: JLong)))
        if (contig.nonEmpty)
          contigs += contig
      }
    }

    val lociSet = LociSet.fromContigs(contigs)
    if (lociSet.nonEmpty)
      _next = Some(lociSet)

    _next.isDefined
  }


  override def head: LociSet = {
    if (!hasNext) throw new NoSuchElementException
    _next.get
  }

  override def next(): LociSet = {
    val n = head
    _next = None
    n
  }
}
