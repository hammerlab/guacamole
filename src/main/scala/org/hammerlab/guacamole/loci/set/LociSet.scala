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

package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.Common

import scala.collection.SortedMap
import scala.collection.immutable.TreeMap

/**
 * An immutable collection of genomic regions on any number of contigs.
 *
 * Used, for example, to keep track of what loci to call variants at.
 *
 * Since contiguous genomic intervals are a common case, this is implemented with sets of (start, end) intervals.
 *
 * All intervals are half open: inclusive on start, exclusive on end.
 *
 * @param map A map from contig-name to Contig, which is a set or genomic intervals as described above.
 */
case class LociSet(private val map: SortedMap[String, Contig]) {

  /** The contigs included in this LociSet with a nonempty set of loci. */
  lazy val contigs = map.values.toSeq

  /** The number of loci in this LociSet. */
  lazy val count: Long = contigs.map(_.count).sum

  def isEmpty = map.isEmpty
  def nonEmpty = map.nonEmpty

  /** Given a contig name, returns a [[Contig]] giving the loci on that contig. */
  def onContig(contig: String): Contig = map.getOrElse(contig, Contig(contig))

  /** Returns a string representation of this LociSet, in the same format that Builder expects. */
  override def toString: String = truncatedString(Int.MaxValue)

  /**
   * String representation, truncated to maxLength characters.
   */
  def truncatedString(maxLength: Int = 500): String = {
    Common.assembleTruncatedString(
      contigs.iterator.flatMap(_.stringPieces),
      maxLength
    )
  }

  override def equals(other: Any) = other match {
    case that: LociSet => map.equals(that.map)
    case _             => false
  }
  override def hashCode = map.hashCode

  /**
   * Split the LociSet into two sets, where the first one has `numToTake` loci, and the second one has the
   * remaining loci.
   *
   * @param numToTake number of elements to take. Must be <= number of elements in the map.
   */
  def take(numToTake: Long): (LociSet, LociSet) = {
    assume(numToTake <= count, s"Can't take $numToTake loci from a set of size $count.")

    // Optimize for taking none or all:
    if (numToTake == 0) {
      (LociSet.empty, this)
    } else if (numToTake == count) {
      (this, LociSet.empty)
    } else {

      /* TODO: may want to optimize this to not fully construct two new maps. Could share singleContig instances between
       * the current map and the split maps, for example.
       */
      val first = new Builder()
      val second = new Builder()
      var remaining = numToTake
      var doneTaking = false

      for {
        contig <- contigs
        range <- contig.ranges
      } {
        if (doneTaking) {
          second.put(contig.name, range.start, range.end)
        } else if (remaining >= range.length) {
          first.put(contig.name, range.start, range.end)
          remaining -= range.length
        } else {
          first.put(contig.name, range.start, range.start + remaining)
          second.put(contig.name, range.start + remaining, range.end)
          doneTaking = true
        }
      }

      val (firstSet, secondSet) = (first.result, second.result)
      assert(firstSet.count == numToTake)
      assert(firstSet.count + secondSet.count == count)
      (firstSet, secondSet)
    }
  }
}

object LociSet {
  /** An empty LociSet. */
  val empty = LociSet(TreeMap.empty[String, Contig])

  /**
   * Return a new builder instance for constructing a LociSet.
   */
  def newBuilder(): Builder = new Builder()

  /** Return a LociSet of a single genomic interval. */
  def apply(contig: String, start: Long, end: Long): LociSet = {
    (new Builder).put(contig, start, end).result
  }

  def all(contigLengths: Map[String, Long]) = Builder.all.result(contigLengths)

  def apply(loci: String): LociSet = Builder(loci).result

  def apply(contigs: Iterable[(String, Contig)]): LociSet = LociSet(TreeMap(contigs.toSeq: _*))
}
