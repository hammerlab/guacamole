package org.hammerlab.guacamole.loci.set

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.lang.{Long => JLong}

import com.google.common.collect.{RangeSet, TreeRangeSet, Range => JRange}
import org.hammerlab.guacamole.reference.{ContigName, Interval, Locus}
import org.hammerlab.guacamole.strings.TruncatedToString

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * A set of loci on a contig, stored/manipulated as loci ranges.
 */
case class Contig(var name: ContigName, private var rangeSet: RangeSet[JLong]) extends TruncatedToString {

  private def readObject(in: ObjectInputStream): Unit = {
    name = in.readUTF()
    val num = in.readInt()
    rangeSet = TreeRangeSet.create[JLong]()
    for {
      i <- 0 until num
    } {
      val start = in.readLong()
      val end = in.readLong()
      val range = JRange.closedOpen[JLong](start, end)
      rangeSet.add(range)
    }
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeUTF(name)
    out.writeInt(ranges.length)
    for {
      Interval(start, end) <- ranges
    } {
      out.writeLong(start)
      out.writeLong(end)
    }
  }

  /** Is the given locus contained in this set? */
  def contains(locus: Locus): Boolean = rangeSet.contains(locus)

  /** This set as a regular scala array of ranges. */
  lazy val ranges: Vector[Interval] = {
    rangeSet
      .asRanges()
      .map(range => Interval(range.lowerEndpoint(), range.upperEndpoint()))
      .toVector
      .sortBy(x => x)
  }

  /** Is this contig empty? */
  def isEmpty: Boolean = rangeSet.isEmpty

  def nonEmpty: Boolean = !isEmpty

  /** Iterator through loci on this contig, sorted. */
  def iterator = new LociIterator(ranges.iterator.buffered)

  /** Number of loci on this contig. */
  def count = ranges.map(_.length).sum

  /** Returns whether a given genomic region overlaps with any loci on this contig. */
  def intersects(start: Long, end: Long): Boolean = !rangeSet.subRangeSet(JRange.closedOpen(start, end)).isEmpty

  /** Intersect this Contig with another */
  def intersect(other: Contig): Contig = {
    val copy = TreeRangeSet.create(rangeSet)

    // Remove all loci not in the other range set
    copy.removeAll(other.rangeSet.complement())

    Contig(
      name,
      copy
    )
  }

  /** Remove the loci of another contig */
  def difference(other: Contig): Contig = {
    val copy = TreeRangeSet.create(rangeSet)

    // Remove all loci not in the other range set
    copy.removeAll(other.rangeSet)

    Contig(
      name,
      copy
    )
  }

  /**
   * Make two new Contigs: one with the first @numToTake loci from this Contig, and the second with the rest.
   *
   * Used by LociSet.take.
   */
  private[set] def take(numToTake: Long): (Contig, Contig) = {
    val firstRanges = ArrayBuffer[Interval]()
    val secondRanges = ArrayBuffer[Interval]()

    var remaining = numToTake
    var doneTaking = false
    for {
      range <- ranges
    } {
      if (doneTaking) {
        secondRanges.append(range)
      } else if (range.length < numToTake) {
        firstRanges.append(range)
        remaining -= range.length
      } else {
        firstRanges.append(Interval(range.start, range.start + remaining))
        secondRanges.append(Interval(range.start + remaining, range.end))
        doneTaking = true
      }
    }

    (Contig(name, firstRanges), Contig(name, secondRanges))
  }

  /**
   * Iterator over string representations of each range in the map, used to assemble (possibly truncated) .toString()
   * output.
   */
  def stringPieces =
    ranges.iterator.map(pair =>
      "%s:%d-%d".format(name, pair.start, pair.end)
    )
}

private[loci] object Contig {
  // Empty-contig constructor, for convenience.
  def apply(name: ContigName): Contig = Contig(name, TreeRangeSet.create[JLong]())

  // Constructors that make a Contig from its name and some ranges.
  def apply(tuple: (ContigName, Iterable[Interval])): Contig = Contig(tuple._1, tuple._2)
  def apply(name: ContigName, ranges: Iterable[Interval]): Contig =
    Contig(
      name,
      {
        val rangeSet = TreeRangeSet.create[JLong]()
        for {
          Interval(start, end) <- ranges
        } {
          rangeSet.add(JRange.closedOpen(start, end))
        }
        rangeSet
      }
    )
}
