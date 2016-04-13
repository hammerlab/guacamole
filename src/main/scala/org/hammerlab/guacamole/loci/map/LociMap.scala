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
import org.hammerlab.guacamole.loci.SimpleRange
import org.hammerlab.guacamole.loci.set.{LociSet, Builder => LociSetBuilder}

import scala.collection.immutable.TreeMap
import scala.collection.{SortedMap, mutable}

/**
 * An immutable map from loci to a instances of an arbitrary type T.
 *
 * Since contiguous genomic intervals are a common case, this is implemented with sets of (start, end) intervals.
 *
 * All intervals are half open: inclusive on start, exclusive on end.
 *
 * @param map Map from contig names to [[Contig]] instances giving the regions and values on that contig.
 */
case class LociMap[T](private val map: SortedMap[String, Contig[T]]) {

  /** The contigs included in this LociMap with a nonempty set of loci. */
  lazy val contigs = map.values.toSeq

  /** The number of loci in this LociMap. */
  lazy val count: Long = contigs.map(_.count).sum

  /** The "inverse map", i.e. a T -> LociSet map that gives the loci that map to each value. */
  lazy val asInverseMap: Map[T, LociSet] = {
    val mapOfBuilders = new mutable.HashMap[T, LociSetBuilder]()
    for {
      contig <- contigs
      (SimpleRange(start, end), value) <- contig.asMap
    } {
      mapOfBuilders
        .getOrElseUpdate(value, LociSet.newBuilder())
        .put(contig.name, start, end)
    }
    mapOfBuilders.mapValues(_.result).toMap
  }

  /**
   * Returns the loci map on the specified contig.
   *
   * @param contig The contig name
   * @return A [[Contig]] instance giving the loci mapping on the specified contig.
   */
  def onContig(contig: String): Contig[T] = map.get(contig) match {
    case Some(result) => result
    case None         => Contig[T](contig, LociMap.emptyRangeMap[T]())
  }

  /** Returns the union of this LociMap with another. */
  def union(other: LociMap[T]): LociMap[T] = {
    LociMap.union(this, other)
  }

  override def toString: String = truncatedString()

  /**
   * String representation, truncated to maxLength characters.
   *
   * If includeValues is true (default), then also include the values mapped to by this LociMap. If it's false,
   * then only the keys are included.
   */
  def truncatedString(maxLength: Int = 500): String = {
    Common.assembleTruncatedString(
      contigs.iterator.flatMap(_.stringPieces),
      maxLength
    )
  }

  override def equals(other: Any) = other match {
    case that: LociMap[T] => map.equals(that.map)
    case _                => false
  }
  override def hashCode = map.hashCode
}

object LociMap {

  /** Make an empty RangeMap of the given type. */
  def empty[T](): LociMap[T] = newBuilder[T]().result

  private def emptyRangeMap[T]() = ImmutableRangeMap.of[JLong, T]()

  /** Returns a new Builder instance for constructing a LociMap. */
  def newBuilder[T](): Builder[T] = new Builder[T]()

  /** Construct an empty LociMap. */
  def apply[T](): LociMap[T] = LociMap(TreeMap[String, Contig[T]]())

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
    for {
      lociMap <- lociMaps
      contig <- lociMap.contigs
      (SimpleRange(start, end), value) <- contig.asMap
    } {
      builder.put(contig.name, start, end, value)
    }
    builder.result
  }
}
