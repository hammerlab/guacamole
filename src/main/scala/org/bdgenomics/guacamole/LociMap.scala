/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.guacamole

import com.google.common.collect._
import scala.collection.immutable.{ SortedMap }
import scala.collection.{ AbstractIterator, Iterator, mutable, JavaConversions }
import com.esotericsoftware.kryo.{ Serializer, Kryo }
import com.esotericsoftware.kryo.io.{ Input, Output }
import scala.Some
import scala.collection.mutable.ArrayBuffer

/**
 * An immutable map from loci to a instances of an arbitrary type T.
 *
 * Since contiguous genomic intervals are a common case, this is implemented with sets of (start, end) intervals.
 *
 * All intervals are half open: inclusive on start, exclusive on end.
 *
 * @param map Map from contig names to [[LociMap.SingleContig]] instances giving the regions and values on that contig.
 */
case class LociMap[T](private val map: Map[String, LociMap.SingleContig[T]]) {

  private val sortedMap = SortedMap[String, LociMap.SingleContig[T]](map.filter(!_._2.isEmpty).toArray: _*)

  /** The contigs included in this LociMap with a nonempty set of loci. */
  lazy val contigs: Seq[String] = sortedMap.keys.toSeq

  /** The number of loci in this LociMap. */
  lazy val count: Long = sortedMap.valuesIterator.map(_.count).sum

  /** Does count == 0? */
  lazy val isEmpty = count == 0

  /** The "inverse map", i.e. a T -> LociSet map that gives the loci that map to each value. */
  lazy val asInverseMap: Map[T, LociSet] = {
    val mapOfBuilders = new mutable.HashMap[T, LociSet.Builder]()
    contigs.foreach(contig => {
      onContig(contig).asMap.foreach({
        case (range, value) => mapOfBuilders.get(value) match {
          case None          => mapOfBuilders.put(value, LociSet.newBuilder.put(contig, range.start, range.end))
          case Some(builder) => builder.put(contig, range.start, range.end)
        }
      })
    })
    mapOfBuilders.mapValues(_.result).toMap
  }

  def filterContigs(function: String => Boolean): LociMap[T] = {
    LociMap(map.filterKeys(function))
  }

  /**
   * Returns the loci map on the specified contig.
   *
   * @param contig The contig name
   * @return A [[LociSet.SingleContig]] instance giving the loci mapping on the specified contig.
   */
  def onContig(contig: String): LociMap.SingleContig[T] = sortedMap.get(contig) match {
    case Some(result) => result
    case None         => LociMap.SingleContig[T](contig, LociMap.emptyRangeMap[T]())
  }

  /** Returns the union of this LociMap with another. */
  def union(other: LociMap[T]): LociMap[T] = {
    LociMap.union(this, other)
  }

  override def toString(): String = truncatedString(Int.MaxValue)

  /**
   * String representation, truncated to maxLength characters.
   *
   * If includeValues is true (default), then also include the values mapped to by this LociMap. If it's false,
   * then only the keys are included.
   */
  def truncatedString(maxLength: Int = 500, includeValues: Boolean = true): String = {
    Common.assembleTruncatedString(
      contigs.iterator.flatMap(contig => onContig(contig).stringPieces(includeValues)),
      maxLength)
  }

  override def equals(other: Any) = other match {
    case that: LociMap[T] => sortedMap.equals(that.sortedMap)
    case _                => false
  }
  override def hashCode = sortedMap.hashCode

  /**
   * Split the LociMap into two maps, where the first one has `numToTake` elements, and the second one has the
   * remaining elements.
   *
   * @param numToTake number of elements to take. Must be <= number of elements in the map.
   */
  def take(numToTake: Long): (LociMap[T], LociMap[T]) = {
    assume(numToTake <= count, "Can't take %d loci from a map of size %d.".format(numToTake, count))

    // Optimize for taking none or all:
    if (numToTake == 0) return (LociMap.empty[T](), this)
    if (numToTake == count) return (this, LociMap.empty[T]())

    /* TODO: may want to optimize this to not fully construct two new maps. Could share singleContig instances between
     * the current map and the split maps, for example.
     */
    val first = LociMap.newBuilder[T]()
    val second = LociMap.newBuilder[T]()
    var remaining = numToTake
    var doneTaking = false
    contigs.foreach(contig => {
      onContig(contig).asMap.foreach({
        case (range, value) => {
          if (doneTaking) {
            second.put(contig, range.start, range.end, value)
          } else {
            if (remaining >= range.length) {
              first.put(contig, range.start, range.end, value)
              remaining -= range.length
            } else {
              first.put(contig, range.start, range.start + remaining, value)
              second.put(contig, range.start + remaining, range.end, value)
              doneTaking = true
            }
          }
        }
      })
    })
    val (firstResult, secondResult) = (first.result, second.result)
    assert(firstResult.count == numToTake)
    assert(firstResult.count + secondResult.count == count)
    (firstResult, secondResult)
  }
}

object LociMap {

  /**
   * A range of Longs. Inclusive on start, exclusive on end.
   */
  case class SimpleRange(start: Long, end: Long) extends Ordered[SimpleRange] {
    /** Iterate through elements in the range. */
    def iterator(): Iterator[Long] = new Iterator[Long] {
      private var i = start
      override def hasNext: Boolean = i < end
      override def next(): Long =
        if (hasNext) { val result = i; i += 1; result }
        else Iterator.empty.next()
    }
    /** Number of elements in the range. */
    def length: Long = end - start

    /** Comparisons between ranges. Order is DESCENDING (i.e. reversed) from by start. */
    def compare(other: SimpleRange): Int = {
      val diff = start - other.start
      if (diff < 0) -1
      else if (diff == 0) 0
      else 1
    }
  }

  /** Make an empty RangeMap of the given type. */
  def empty[T](): LociMap[T] = newBuilder[T]().result

  // We're using Google's guava library, which is Java. We have to use java integer's instead of Scala's.
  private type JLong = java.lang.Long
  private def emptyRangeMap[T]() = ImmutableRangeMap.of[JLong, T]()

  /** Returns a new Builder instance for constructing a LociMap. */
  def newBuilder[T](): Builder[T] = new Builder[T]()

  /** Class for building a LociMap */
  class Builder[T] {
    val data = new mutable.HashMap[String, mutable.ArrayBuffer[(Long, Long, T)]]()

    /** Set the value at the given locus range in the LociMap under construction. */
    def put(contig: String, start: Long, end: Long, value: T): Builder[T] = {
      assume(end >= start)
      if (end > start) data.get(contig) match {
        case None              => data.put(contig, ArrayBuffer[(Long, Long, T)]((start, end, value)))
        case Some(arrayBuffer) => arrayBuffer += ((start, end, value))
      }
      this
    }

    /** Set the value for all loci in the given LociSet to the specified value in the LociMap under construction. */
    def put(loci: LociSet, value: T): Builder[T] = {
      loci.contigs.foreach(contig => {
        loci.onContig(contig).ranges.foreach(range => {
          put(contig, range.start, range.end, value)
        })
      })
      this
    }

    /** Build the result. */
    def result(): LociMap[T] = {
      LociMap[T](data.map({
        case (contig, array) => {
          val mutableRangeMap = TreeRangeMap.create[JLong, T]()
          // We combine adjacent or overlapping intervals with the same value into one interval.
          val iterator = array.sortBy(_._1).iterator.buffered
          while (iterator.hasNext) {
            var (start: Long, end: Long, value) = iterator.next()
            while (iterator.hasNext && iterator.head._3 == value && iterator.head._1 <= end) {
              end = iterator.next()._2
            }
            mutableRangeMap.put(Range.closedOpen[JLong](start, end), value)
          }
          contig -> SingleContig(contig, mutableRangeMap)
        }
      }).toMap)
    }
  }

  /** Construct an empty LociMap. */
  def apply[T](): LociMap[T] = LociMap[T](Map[String, SingleContig[T]]())

  /** Return a LociMap of a single genomic interval. */
  def apply[T](contig: String, start: Long, end: Long, value: T): LociMap[T] = {
    (new Builder[T]).put(contig, start, end, value).result
  }

  /** Returns union of specified [[LociMap]] instances. */
  def union[T](lociMaps: LociMap[T]*): LociMap[T] = {
    val builder = LociMap.newBuilder[T]
    lociMaps.foreach(lociMap => {
      lociMap.contigs.foreach(contig => {
        lociMap.onContig(contig).asMap.foreach(pair => {
          builder.put(contig, pair._1.start, pair._1.end, pair._2)
        })
      })
    })
    builder.result
  }

  /**
   * A map from loci to instances of an arbitrary type where the loci are all on the same contig.
   * @param contig The contig name
   * @param rangeMap The range map of loci intervals -> values.
   */
  case class SingleContig[T](contig: String, private val rangeMap: RangeMap[JLong, T]) {

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
      JavaConversions.asScalaIterator(rangeMap.subRangeMap(range).asMapOfRanges.values.iterator).toSet
    }

    /** Does this map contain the given locus? */
    def contains(locus: Long): Boolean = get(locus).isDefined

    /** This map as a regular scala immutable map from exclusive numeric ranges to values. */
    lazy val asMap: SortedMap[SimpleRange, T] = {
      val result = JavaConversions.mapAsScalaMap(rangeMap.asMapOfRanges).map(
        pair => (SimpleRange(pair._1.lowerEndpoint, pair._1.upperEndpoint), pair._2))
      scala.collection.immutable.TreeMap[SimpleRange, T](result.toSeq: _*)
    }

    /** Number of loci in this map. */
    lazy val count: Long = ranges.toIterator.map(_.length).sum

    /** Returns a sequence of ranges giving the intervals of this map. */
    lazy val ranges: Iterable[SimpleRange] = asMap.keys

    /** Number of ranges in this map. */
    lazy val numRanges: Long = rangeMap.asMapOfRanges.size.toLong

    /** Is this map empty? */
    lazy val isEmpty: Boolean = asMap.isEmpty

    /** Iterator through loci in this map, sorted. */
    def lociIndividually(): Iterator[Long] = ranges.iterator.flatMap(_.iterator)

    /** Returns the union of this map with another. Both must be on the same contig. */
    def union(other: SingleContig[T]): SingleContig[T] = {
      assume(contig == other.contig,
        "Tried to union two LociMap.SingleContig on different contigs: %s and %s".format(contig, other.contig))
      val both = TreeRangeMap.create[JLong, T]()
      both.putAll(rangeMap)
      both.putAll(other.rangeMap)
      SingleContig(contig, both)
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
        if (includeValues) "%s:%d-%d=%s".format(contig, pair._1.start, pair._1.end, pair._2.toString)
        else "%s:%d-%d".format(contig, pair._1.start, pair._1.end)
      })
    }

    override def toString(): String = truncatedString(Int.MaxValue)
  }
}

// Serialization: currently only support LociMap[Long].
class LociMapLongSerializer extends Serializer[LociMap[Long]] {
  def write(kryo: Kryo, output: Output, obj: LociMap[Long]) = {
    output.writeLong(obj.contigs.length)
    obj.contigs.foreach(contig => {
      kryo.writeObject(output, obj.onContig(contig))
    })
  }
  def read(kryo: Kryo, input: Input, klass: Class[LociMap[Long]]): LociMap[Long] = {
    val count: Long = input.readLong()
    val pairs = (0L until count).map(i => {
      val obj = kryo.readObject(input, classOf[LociMap.SingleContig[Long]])
      obj.contig -> obj
    })
    LociMap[Long](Map[String, LociMap.SingleContig[Long]](pairs: _*))
  }
}
class LociMapLongSingleContigSerializer extends Serializer[LociMap.SingleContig[Long]] {
  def write(kryo: Kryo, output: Output, obj: LociMap.SingleContig[Long]) = {
    output.writeString(obj.contig.toCharArray)
    output.writeLong(obj.asMap.size)
    obj.asMap.foreach({
      case (range, value) => {
        output.writeLong(range.start)
        output.writeLong(range.end)
        output.writeLong(value)
      }
    })
  }
  def read(kryo: Kryo, input: Input, klass: Class[LociMap.SingleContig[Long]]): LociMap.SingleContig[Long] = {
    val builder = LociMap.newBuilder[Long]()
    val contig = input.readString()
    val count = input.readLong()
    (0L until count).foreach(i => {
      val start = input.readLong()
      val end = input.readLong()
      val value = input.readLong()
      builder.put(contig, start, end, value)
    })
    builder.result.onContig(contig)
  }
}

