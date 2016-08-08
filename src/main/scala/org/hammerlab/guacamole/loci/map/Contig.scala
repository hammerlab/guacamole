package org.hammerlab.guacamole.loci.map

import java.lang.{Long => JLong}

import com.google.common.collect.{RangeMap, TreeRangeMap, Range => JRange}
import org.hammerlab.guacamole.loci.set.{Contig => LociSetContig}
import org.hammerlab.guacamole.reference.{ContigName, Interval, Locus}
import org.hammerlab.guacamole.strings.TruncatedToString

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
case class Contig[T](name: ContigName, private val rangeMap: RangeMap[JLong, T]) extends TruncatedToString {

  /**
   * Get the value associated with the given locus. Returns Some(value) if the given locus is in this map, None
   * otherwise.
   */
  def get(locus: Locus): Option[T] = {
    Option(rangeMap.get(locus))
  }

  /**
   * Given a loci interval, return the set of all values mapped to by any loci in the interval.
   */
  def getAll(start: Long, end: Long): Set[T] = {
    val range = JRange.closedOpen[JLong](start, end)
    rangeMap.subRangeMap(range).asMapOfRanges.values.iterator.toSet
  }

  /** This map as a regular scala immutable map from exclusive numeric ranges to values. */
  lazy val asMap: SortedMap[Interval, T] = {
    TreeMap(
      (for {
        (range, value) <- rangeMap.asMapOfRanges.toSeq
      } yield {
        Interval(range) -> value
      }): _*
    )
  }

  /**
   * Map from each value found in this Contig to a LociSet Contig representing the loci that map to that value.
   */
  lazy val inverse: Map[T, LociSetContig] = {
    val map = mutable.HashMap[T, ArrayBuffer[JRange[JLong]]]()
    for {
      (range, value) <- asMap
    } {
      map
        .getOrElseUpdate(value, ArrayBuffer())
        .append(range.toJavaRange)
    }
    map.mapValues(ranges => LociSetContig(name, ranges)).toMap
  }

  /** Number of loci on this contig; exposed only to LociMap. */
  private[map] lazy val count = asMap.keysIterator.map(_.length).sum

  /**
   * Iterator over string representations of each range in the map.
   */
  def stringPieces = {
    for {
      (Interval(start, end), value) <- asMap.iterator
    } yield {
      "%s:%d-%d=%s".format(name, start, end, value)
    }
  }
}

object Contig {

  def apply[T](name: String): Contig[T] = Contig(name, TreeRangeMap.create[JLong, T]())

  /** Convenience constructors for making a Contig from a name and some loci ranges. */
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

      mutableRangeMap.put(JRange.closedOpen[JLong](start, end), value)
    })
    Contig(name, mutableRangeMap)
  }
}
