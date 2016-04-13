package org.hammerlab.guacamole.loci.map

import java.lang.{Long => JLong}

import com.google.common.collect.{Range, RangeMap, TreeRangeMap}
import org.hammerlab.guacamole.Common
import org.hammerlab.guacamole.loci.SimpleRange

import scala.collection.Iterator
import scala.collection.JavaConversions._
import scala.collection.immutable.{SortedMap, TreeMap}

/**
 * A map from loci to instances of an arbitrary type where the loci are all on the same contig.
 *
 * @param contig The contig name
 * @param rangeMap The range map of loci intervals -> values.
 */
case class Contig[T](contig: String, private val rangeMap: RangeMap[JLong, T]) {

  /**
   * Get the value associated with the given locus. Returns Some(value) if the given locus is in this map, None
   * otherwise.
   */
  def get(locus: Long): Option[T] = {
    Option(rangeMap.get(locus))
  }

  /**
   * Given a loci interval, return the set of all values mapped to by any loci in the interval.
   */
  def getAll(start: Long, end: Long): Set[T] = {
    val range = Range.closedOpen[JLong](start, end)
    rangeMap.subRangeMap(range).asMapOfRanges.values.iterator.toSet
  }

  /** Does this map contain the given locus? */
  def contains(locus: Long): Boolean = get(locus).isDefined

  /** This map as a regular scala immutable map from exclusive numeric ranges to values. */
  lazy val asMap: SortedMap[SimpleRange, T] = {
    TreeMap(
      (for {
        (range, value) <- rangeMap.asMapOfRanges.toSeq
      } yield {
        SimpleRange(range) -> value
      }): _*
    )
  }

  /** Number of loci in this map. */
  lazy val count = ranges.map(_.length).sum

  /** Returns a sequence of ranges giving the intervals of this map. */
  lazy val ranges: Array[SimpleRange] = asMap.keys.toArray

  /** Number of ranges in this map. */
  lazy val numRanges: Long = rangeMap.asMapOfRanges.size.toLong

  /** Is this map empty? */
  lazy val isEmpty: Boolean = asMap.isEmpty

  /** Iterator through loci in this map, sorted. */
  def lociIndividually(): Iterator[Long] = ranges.iterator.flatMap(_.iterator)

  /** Returns the union of this map with another. Both must be on the same contig. */
  def union(other: Contig[T]): Contig[T] = {
    assume(contig == other.contig,
      "Tried to union two LociMap.Contig on different contigs: %s and %s".format(contig, other.contig))
    val both = TreeRangeMap.create[JLong, T]()
    both.putAll(rangeMap)
    both.putAll(other.rangeMap)
    Contig(contig, both)
  }

  /**
   * String representation, truncated to maxLength characters.
   *
   * If includeValues is true (default), then also include the values mapped to by this LociMap. If it's false,
   * then only the keys are included.
   */
  def truncatedString(maxLength: Int = 100, includeValues: Boolean = true): String = {
    Common.assembleTruncatedString(stringPieces(includeValues), maxLength)
  }

  /**
   * Iterator over string representations of each range in the map.
   *
   * If includeValues is true (default), then also include the values mapped to by this LociMap. If it's false,
   * then only the keys are included.
   */
  def stringPieces(includeValues: Boolean = true) = {
    asMap.iterator.map(pair => {
      if (includeValues)
        "%s:%d-%d=%s".format(contig, pair._1.start, pair._1.end, pair._2.toString)
      else
        "%s:%d-%d".format(contig, pair._1.start, pair._1.end)
    })
  }

  override def toString: String = truncatedString(Int.MaxValue)
}
