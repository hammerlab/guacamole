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

import org.hammerlab.guacamole.loci.set.{LociSet, Builder => LociSetBuilder}
import org.hammerlab.guacamole.strings.TruncatedToString

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
case class LociMap[T](private val map: SortedMap[String, Contig[T]]) extends TruncatedToString {

  /** The contigs included in this LociMap with a nonempty set of loci. */
  lazy val contigs = map.values.toSeq

  /** The number of loci in this LociMap. */
  lazy val count: Long = contigs.map(_.count).sum

  /** The "inverse map", i.e. a T -> LociSet map that gives the loci that map to each value. */
  lazy val inverse: Map[T, LociSet] = {
    val mapOfBuilders = new mutable.HashMap[T, LociSetBuilder]()
    for {
      contig <- contigs
      (value, setContig) <- contig.inverse
    } {
      mapOfBuilders
        .getOrElseUpdate(value, new LociSetBuilder)
        .add(setContig)
    }
    mapOfBuilders.mapValues(_.result).toMap
  }

  /**
   * Returns the loci map on the specified contig.
   *
   * @param contig The contig name
   * @return A [[Contig]] instance giving the loci mapping on the specified contig.
   */
  def onContig(contig: String): Contig[T] = map.getOrElse(contig, Contig[T](contig))

  /** Build a truncate-able toString() out of underlying contig pieces. */
  def stringPieces: Iterator[String] = contigs.iterator.flatMap(_.stringPieces)
}

object LociMap {
  /** Returns a new Builder instance for constructing a LociMap. */
  def newBuilder[T]: Builder[T] = new Builder[T]()

  /** Construct an empty LociMap. */
  def apply[T](): LociMap[T] = LociMap(TreeMap[String, Contig[T]]())

  /** The following convenience constructors are only called by Builder. */
  private[map] def apply[T](contigs: (String, Long, Long, T)*): LociMap[T] = {
    val builder = new Builder[T]
    for {
      (contig, start, end, value) <- contigs
    } {
      builder.put(contig, start, end, value)
    }
    builder.result()
  }

  private[map] def apply[T](contigs: Iterable[Contig[T]]): LociMap[T] =
    LociMap(
      TreeMap(
        contigs.map(contig => contig.name -> contig).toSeq: _*
      )
    )
}
