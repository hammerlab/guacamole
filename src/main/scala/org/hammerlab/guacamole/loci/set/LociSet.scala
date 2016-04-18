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

import org.hammerlab.guacamole.loci.map.LociMap

/**
 * An immutable collection of genomic regions on any number of contigs.
 *
 * Used, for example, to keep track of what loci to call variants at.
 *
 * Since contiguous genomic intervals are a common case, this is implemented with sets of (start, end) intervals.
 *
 * All intervals are half open: inclusive on start, exclusive on end.
 *
 * We implement a set by wrapping a LociMap[Long], and ignoring the values of the map.
 *
 * @param map LociMap[Long] instance. The values are ignored, and the keys are the members of the LociSet.
 */
case class LociSet(map: LociMap[Long]) {

  /** The contigs included in this LociSet with a nonempty set of loci. */
  def contigs: Seq[String] = map.contigs

  /** The number of loci in this LociSet. */
  def count: Long = map.count

  /** Does count == 0? */
  def isEmpty = map.isEmpty
  def nonEmpty = !isEmpty

  /** Given a contig name, returns a [[Contig]] giving the loci on that contig. */
  def onContig(contig: String): Contig = Contig(map.onContig(contig))

  /** Returns the union of this LociSet with another. */
  def union(other: LociSet): LociSet = LociSet(map.union(other.map))

  /** Returns a string representation of this LociSet, in the same format that Builder expects. */
  override def toString: String = truncatedString(Int.MaxValue)

  /** String representation, truncated to maxLength characters. */
  def truncatedString(maxLength: Int = 200): String = map.truncatedString(maxLength, includeValues = false)

  /** Returns a LociSet containing only those contigs TODO*/
  def filterContigs(function: String => Boolean): LociSet = {
    LociSet(map.filterContigs(function))
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
      val mapTake = map.take(numToTake)
      (LociSet(mapTake._1), LociSet(mapTake._2))
    }
  }
}

object LociSet {
  /** An empty LociSet. */
  val empty = LociSet(LociMap[Long]())

  /**
   * Return a new builder instance for constructing a LociSet.
   */
  def newBuilder(): Builder = new Builder()

  /** Return a LociSet of a single genomic interval. */
  def apply(contig: String, start: Long, end: Long): LociSet = {
    (new Builder).put(contig, start, end).result
  }

  /** Returns union of specified [[LociSet]] instances. */
  def union(lociSets: LociSet*): LociSet = {
    val wrapped = LociMap.newBuilder[Long]
    lociSets.foreach(lociSet => {
      wrapped.put(lociSet, 0)
    })
    LociSet(wrapped.result)
  }

  def all(contigLengths: Map[String, Long]) = Builder.all.result(contigLengths)

  def apply(loci: String): LociSet = Builder(loci).result
}

