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

package org.hammerlab.guacamole.loci.map

import java.lang.{Long => JLong}

import com.google.common.collect._
import org.hammerlab.guacamole.Common
import org.hammerlab.guacamole.loci.set
import org.hammerlab.guacamole.loci.set.LociSet

import scala.collection.immutable.SortedMap
import scala.collection.mutable

/**
 * An immutable map from loci to a instances of an arbitrary type T.
 *
 * Since contiguous genomic intervals are a common case, this is implemented with sets of (start, end) intervals.
 *
 * All intervals are half open: inclusive on start, exclusive on end.
 *
 * @param map Map from contig names to [[Contig]] instances giving the regions and values on that contig.
 */
case class LociMap[T](private val map: Map[String, Contig[T]]) {

  private val sortedMap = SortedMap[String, Contig[T]](map.filter(!_._2.isEmpty).toArray: _*)

  /** The contigs included in this LociMap with a nonempty set of loci. */
  lazy val contigs: Seq[String] = sortedMap.keys.toSeq

  /** The number of loci in this LociMap. */
  lazy val count: Long = sortedMap.valuesIterator.map(_.count).sum

  /** Does count == 0? */
  lazy val isEmpty = count == 0

  /** The "inverse map", i.e. a T -> LociSet map that gives the loci that map to each value. */
  lazy val asInverseMap: Map[T, LociSet] = {
    val mapOfBuilders = new mutable.HashMap[T, set.Builder]()
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
   * @return A [[Contig]] instance giving the loci mapping on the specified contig.
   */
  def onContig(contig: String): Contig[T] = sortedMap.get(contig) match {
    case Some(result) => result
    case None         => Contig[T](contig, LociMap.emptyRangeMap[T]())
  }

  /** Returns the union of this LociMap with another. */
  def union(other: LociMap[T]): LociMap[T] = {
    LociMap.union(this, other)
  }

  override def toString: String = truncatedString(Int.MaxValue)

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

  /** Make an empty RangeMap of the given type. */
  def empty[T](): LociMap[T] = newBuilder[T]().result

  private def emptyRangeMap[T]() = ImmutableRangeMap.of[JLong, T]()

  /** Returns a new Builder instance for constructing a LociMap. */
  def newBuilder[T](): Builder[T] = new Builder[T]()

  /** Construct an empty LociMap. */
  def apply[T](): LociMap[T] = LociMap[T](Map[String, Contig[T]]())

  /** Return a LociMap of a single genomic interval. */
  def apply[T](contig: String, start: Long, end: Long, value: T): LociMap[T] = {
    (new Builder[T]).put(contig, start, end, value).result
  }

  /** Return a LociMap of a single genomic interval. */
  def apply[T](contigs: (String, Long, Long, T)*): LociMap[T] = {
    val builder = new Builder[T]
    for {
      (contig, start, end, value) <- contigs
    } {
      builder.put(contig, start, end, value)
    }
    builder.result()
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
}
