package org.hammerlab.guacamole.loci.map

import java.lang.{Long => JLong}

import com.google.common.collect.{Range => JRange}

import com.google.common.collect.{Range, RangeMap, TreeRangeMap}
import org.hammerlab.guacamole.Common
import org.hammerlab.guacamole.loci.SimpleRange
import org.hammerlab.guacamole.loci.set.{Contig => LociSetContig}

import scala.collection.JavaConversions._
import scala.collection.immutable.{SortedMap, TreeMap}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * A map from loci to instances of an arbitrary type where the loci are all on the same contig.
 *
 * @param name The contig name
 * @param rangeMap The range map of loci intervals -> values.
 */
case class Contig[T](name: String, private val rangeMap: RangeMap[JLong, T] = TreeRangeMap.create[JLong, T]()) {

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

  lazy val inverse: Map[T, LociSetContig] = {
    val map = mutable.HashMap[T, ArrayBuffer[JRange[JLong]]]()
    for {
      (range, value) <- asMap
    } {
      map
        .getOrElseUpdate(value, ArrayBuffer())
        .append(range.toJavaRange)
    }
    map.mapValues(ranges => LociSetContig.apply(name, ranges)).toMap
  }

  /** Number of loci on this contig. */
  private[map] lazy val count = asMap.keysIterator.map(_.length).sum

  override def toString: String = truncatedString(Int.MaxValue)

  /**
   * String representation, truncated to maxLength characters.
   *
   * If includeValues is true (default), then also include the values mapped to by this LociMap. If it's false,
   * then only the keys are included.
   */
  private def truncatedString(maxLength: Int = 100): String = {
    Common.assembleTruncatedString(stringPieces, maxLength)
  }

  /**
   * Iterator over string representations of each range in the map.
   */
  private[map] def stringPieces = {
    for {
      (SimpleRange(start, end), value) <- asMap.iterator
    } yield {
      "%s:%d-%d=%s".format(name, start, end, value)
    }
  }
}

object Contig {
  def apply[T](tuple: (String, Iterable[(JLong, JLong, T)])): Contig[T] = apply(tuple._1, tuple._2)
  def apply[T](name: String, ranges: Iterable[(JLong, JLong, T)]): Contig[T] = {
    val mutableRangeMap = TreeRangeMap.create[JLong, T]()
    ranges.foreach(item => {
      var (start, end, value) = item

      // If there is an existing entry associated *with the same value* in the map immediately before the range
      // we're adding, we coalesce the two ranges by setting our start to be its start.
      val existingStart = mutableRangeMap.getEntry(start - 1)
      if (existingStart != null && existingStart.getValue == value) {
        assert(existingStart.getKey.lowerEndpoint < start)
        start = existingStart.getKey.lowerEndpoint
      }

      // Likewise for the end of the range.
      val existingEnd = mutableRangeMap.getEntry(end)
      if (existingEnd != null && existingEnd.getValue == value) {
        assert(existingEnd.getKey.upperEndpoint > end)
        end = existingEnd.getKey.upperEndpoint
      }

      mutableRangeMap.put(Range.closedOpen[JLong](start, end), value)
    })
    Contig(name, mutableRangeMap)
  }
}
