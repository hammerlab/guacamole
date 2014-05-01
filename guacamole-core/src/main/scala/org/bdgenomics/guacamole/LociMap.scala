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
import scala.collection.immutable.{ SortedMap, NumericRange }
import scala.collection.{ mutable, JavaConversions }
import com.esotericsoftware.kryo.{ Serializer, Kryo }
import com.esotericsoftware.kryo.io.{ Input, Output }
import scala.{ collection, Some }
import scala.collection.immutable.NumericRange.Exclusive
import sun.reflect.generics.reflectiveObjects.NotImplementedException
import scala.collection.mutable.ArrayBuffer

/**
 * A collection of genomic regions. Maps reference names (contig names) to a set of loci on that contig.
 *
 * Used, for example, to keep track of what loci to call variants at.
 *
 * Since contiguous genomic intervals are a common case, this is implemented with sets of (start, end) intervals.
 *
 * All intervals are half open: inclusive on start, exclusive on end.
 *
 * @param map Map from contig names to the range set giving the loci under consideration on that contig.
 */
case class LociMap[T](private val map: Map[String, LociMap.SingleContig[T]]) {

  private val sortedMap = SortedMap[String, LociMap.SingleContig[T]](map.filter(!_._2.isEmpty).toArray: _*)

  /** The contigs included in this LociMap with a nonempty set of loci. */
  lazy val contigs: Seq[String] = sortedMap.keys.toSeq

  /** The number of loci in this LociMap. */
  lazy val count: Long = sortedMap.valuesIterator.map(_.count).sum

  lazy val asInverseMap: Map[T, LociSet] = {
    val mapOfBuilders = new mutable.HashMap[T, LociSet.Builder]()
    contigs.foreach(contig => {
      onContig(contig).asMap.foreach({
        case (range, value) => {
          mapOfBuilders.getOrElseUpdate(value, LociSet.newBuilder).put(contig, range.start, range.end)
        }
      })
    })
    mapOfBuilders.mapValues(_.result).toMap
  }

  /**
   * Returns the loci on the specified contig.
   *
   * @param contig The contig name
   * @return A [[LociSet.SingleContig]] instance giving the loci on the specified contig.
   */
  def onContig(contig: String): LociMap.SingleContig[T] = sortedMap.get(contig) match {
    case Some(result) => result
    case None         => LociMap.SingleContig[T](contig, LociMap.emptyRangeMap[T]())
  }

  /** Returns the union of this LociMap with another. */
  def union(other: LociMap[T]): LociMap[T] = {
    val keys = Set[String](contigs: _*).union(Set[String](other.contigs: _*))
    val pairs = keys.map(contig => contig -> onContig(contig).union(other.onContig(contig)))
    LociMap[T](pairs.toMap)
  }

  override def toString(): String = contigs.map(onContig(_).toString).mkString(",")

  override def equals(other: Any) = other match {
    case that: LociMap[T] => sortedMap.equals(that.sortedMap)
    case _                => false
  }
  override def hashCode = sortedMap.hashCode

}
object LociMap {
  private implicit object RangeOrdering extends Ordering[NumericRange.Exclusive[Long]] {
    def compare(o1: NumericRange.Exclusive[Long], o2: NumericRange.Exclusive[Long]) = (o1.start - o2.start).toInt
  }
  private type JLong = java.lang.Long
  private def emptyRangeMap[T]() = ImmutableRangeMap.of[JLong, T]()

  def newBuilder[T](): Builder[T] = new Builder[T]()
  class Builder[T] {
    val data = new mutable.HashMap[String, mutable.ArrayBuffer[(Long, Long, T)]]()
    def put(contig: String, start: Long, end: Long, value: T): Builder[T] = {
      assume(end >= start)
      if (end > start) data.getOrElseUpdate(contig, new ArrayBuffer[(Long, Long, T)]) += ((start, end, value))
      this
    }
    def result(): LociMap[T] = {
      LociMap[T](data.map({
        case (contig, array) => {
          val rangeMapBuilder = ImmutableRangeMap.builder[JLong, T]()
          for ((start, end, value) <- array) {
            rangeMapBuilder.put(Range.closedOpen[JLong](start, end), value)
          }
          contig -> SingleContig(contig, rangeMapBuilder.build)
        }
      }).toMap)
    }
  }

  def apply[T](): LociMap[T] = LociMap[T](Map[String, SingleContig[T]]())

  /** Return a LociMap of a single genomic interval. */
  def apply[T](contig: String, start: Long, end: Long, value: T): LociMap[T] = {
    (new Builder[T]).put(contig, start, end, value).result
  }

  /** Returns union of specified [[LociSet]] instances. */
  def union[T](lociMaps: LociMap[T]*): LociMap[T] = {
    lociMaps.reduce(_.union(_))
  }

  /**
   * A set of loci on a single contig.
   * @param contig The contig name
   * @param rangeMap The range set of loci on this contig.
   */
  case class SingleContig[T](contig: String, rangeMap: RangeMap[JLong, T]) {

    def get(locus: Long): Option[T] = {
      Option(rangeMap.get(locus))
    }
    def getAll(start: Long, end: Long): Set[T] = {
      val range = Range.closedOpen[JLong](start, end)
      JavaConversions.asScalaIterator(rangeMap.subRangeMap(range).asMapOfRanges.values.iterator).toSet
    }

    def contains(locus: Long): Boolean = get(locus).isDefined

    lazy val asMap: SortedMap[Exclusive[Long], T] = {
      val result = JavaConversions.mapAsScalaMap(rangeMap.asMapOfRanges).map(
        pair => (NumericRange[Long](pair._1.lowerEndpoint, pair._1.upperEndpoint, 1), pair._2))
      scala.collection.immutable.TreeMap[Exclusive[Long], T](result.toSeq: _*)
    }

    lazy val count: Long = ranges.map(_.length).sum

    /** Returns a sequence of ranges giving the intervals of this set. */
    lazy val ranges: Iterable[NumericRange[Long]] = asMap.keys

    /** Is this set empty? */
    lazy val isEmpty: Boolean = asMap.isEmpty

    /** Iterator through loci in this map, sorted. */
    def lociIndividually(): Iterator[Long] = ranges.iterator.flatMap(_.iterator)

    /** Returns the union of this set with another. Both must be on the same contig. */
    def union(other: SingleContig[T]): SingleContig[T] = {
      assume(contig == other.contig)
      val both = TreeRangeMap.create[JLong, T]()
      both.putAll(rangeMap)
      both.putAll(other.rangeMap)
      SingleContig(contig, both)
    }

    override def toString(): String = {
      asMap.map(pair => "%s:%d-%d=%s".format(contig, pair._1.start, pair._1.end, pair._2.toString)).mkString(",")
    }
  }
}

// Serialization
// TODO
/*
class LociMapSerializer extends Serializer[LociMap] {
  def write(kyro: Kryo, output: Output, obj: LociMap) = {
    throw new NotImplementedException
  }
  def read(kryo: Kryo, input: Input, klass: Class[LociMap]): LociMap = {
    throw new NotImplementedException
  }
}
class LociMapSingleContigSerializer extends Serializer[LociMap.SingleContig] {
  def write(kyro: Kryo, output: Output, obj: LociMap.SingleContig) = {
    throw new NotImplementedException
  }
  def read(kryo: Kryo, input: Input, klass: Class[LociMap.SingleContig]): LociMap.SingleContig = {
    throw new NotImplementedException
  }
}
*/

