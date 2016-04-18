package org.hammerlab.guacamole.loci.set

import java.lang.{Long => JLong}

import com.google.common.collect.{RangeSet, TreeRangeSet, Range => JRange}
import org.hammerlab.guacamole.loci.SimpleRange
import org.hammerlab.guacamole.strings.TruncatedToString

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

case class Contig(name: String, private val rangeSet: RangeSet[JLong]) extends TruncatedToString {

  /** Is the given locus contained in this set? */
  def contains(locus: Long): Boolean = rangeSet.contains(locus)

  /** This set as a regular scala array of ranges. */
  lazy val ranges: Array[SimpleRange] = {
    rangeSet
      .asRanges()
      .map(SimpleRange(_))
      .toArray
      .sortBy(x => x)
  }

  /** Is this contig empty? */
  def isEmpty: Boolean = rangeSet.isEmpty

  /** Iterator through loci on this contig, sorted. */
  def iterator = new ContigIterator(this)

  /** Number of loci on this contig. */
  def count = ranges.map(_.length).sum

  /** Returns whether a given genomic region overlaps with any loci on this contig. */
  def intersects(start: Long, end: Long) = !rangeSet.subRangeSet(JRange.closedOpen(start, end)).isEmpty

  /**
    * Make two new Contigs: one with the first @numToTake loci from this Contig, and the second with the rest.
    *
    * Used by LociSet.take.
    */
  private[set] def take(numToTake: Long): (Contig, Contig) = {
    val firstRanges = ArrayBuffer[JRange[JLong]]()
    val secondRanges = ArrayBuffer[JRange[JLong]]()

    var remaining = numToTake
    var doneTaking = false
    for {
      range <- ranges
    } {
      if (doneTaking) {
        secondRanges.append(range.toJavaRange)
      } else if (range.length < numToTake) {
        firstRanges.append(range.toJavaRange)
        remaining -= range.length
      } else {
        firstRanges.append(JRange.closedOpen(range.start, range.start + remaining))
        secondRanges.append(JRange.closedOpen(range.start + remaining, range.end))
        doneTaking = true
      }
    }

    (Contig(name, firstRanges), Contig(name, secondRanges))
  }

  /**
    * Iterator over string representations of each range in the map, used to assemble (possibly truncated) .toString()
    * output.
    */
  def stringPieces = {
    ranges.iterator.map(pair =>
      "%s:%d-%d".format(name, pair.start, pair.end)
    )
  }
}

private[loci] object Contig {
  def apply(name: String): Contig = Contig(name, TreeRangeSet.create[JLong]())

  def apply(tuple: (String, Iterable[JRange[JLong]])): Contig = Contig(tuple._1, tuple._2)
  def apply(name: String, ranges: Iterable[JRange[JLong]]): Contig =
    Contig(
      name,
      {
        val rangeSet = TreeRangeSet.create[JLong]()
        ranges.foreach(rangeSet.add)
        rangeSet
      }
    )
}
