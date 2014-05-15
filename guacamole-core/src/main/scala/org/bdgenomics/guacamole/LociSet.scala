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

import com.esotericsoftware.kryo.{ Serializer, Kryo }
import com.esotericsoftware.kryo.io.{ Input, Output }
import org.bdgenomics.guacamole.LociMap.SimpleRange

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
case class LociSet(val map: LociMap[Long]) {

  /** The contigs included in this LociSet with a nonempty set of loci. */
  lazy val contigs: Seq[String] = map.contigs

  /** The number of loci in this LociSet. */
  lazy val count: Long = map.count

  /** Does the LociSet contain any loci? */
  lazy val nonEmpty = map.nonEmpty
  lazy val empty = map.empty

  /** Given a contig name, returns a [[LociSet.SingleContig]] giving the loci on that contig. */
  def onContig(contig: String): LociSet.SingleContig = LociSet.SingleContig(map.onContig(contig))

  /** Returns the union of this LociSet with another. */
  def union(other: LociSet): LociSet = LociSet(map.union(other.map))

  /** Returns a string representation of this LociSet, in the same format that LociSet.parse expects. */
  override def toString(): String = truncatedString(Int.MaxValue)

  /** String representation, truncated to maxLength characters. */
  def truncatedString(maxLength: Int = 100): String = map.truncatedString(maxLength, false)

  override def equals(other: Any) = other match {
    case that: LociSet => map.equals(that.map)
    case _             => false
  }
  override def hashCode = map.hashCode

  /**
   * Split the LociSet into two sets, where the first one has exactly `numToTake` elements, and the second one has the
   * remaining elements.
   */
  def take(numToTake: Long): (LociSet, LociSet) = {
    // Optimize for taking none or all:
    if (numToTake == 0) {
      (LociSet.empty, this)
    } else if (numToTake >= count) {
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

  /**
   * Class for constructing a LociSet.
   */
  class Builder {
    val wrapped = new LociMap.Builder[Long]

    /**
     * Add an interval to the LociSet under construction.
     */
    def put(contig: String, start: Long, end: Long): Builder = {
      wrapped.put(contig, start, end, 0)
      this
    }

    /**
     * Build the result.
     */
    def result(): LociSet = LociSet(wrapped.result)
  }

  /** Return a LociSet of a single genomic interval. */
  def apply(contig: String, start: Long, end: Long): LociSet = {
    (new Builder).put(contig, start, end).result
  }

  /**
   * Return a LociSet parsed from a string representation.
   *
   * @param loci A string of the form "CONTIG:START-END,CONTIG:START-END,..." where CONTIG is a string giving the
   *             contig name, and START and END are integers. Spaces are ignored.
   */
  def parse(loci: String): LociSet = {
    val syntax = """^([\pL\pN._]+):(\pN+)-(\pN+)""".r
    val sets = loci.replace(" ", "").split(',').map({
      case ""                       => LociSet.empty
      case syntax(name, start, end) => LociSet(name, start.toLong, end.toLong)
      case other                    => throw new IllegalArgumentException("Couldn't parse loci range: %s".format(other))
    })
    union(sets: _*)
  }

  /** Returns union of specified [[LociSet]] instances. */
  def union(lociSets: LociSet*): LociSet = {
    lociSets.reduce(_.union(_))
  }

  /**
   * A set of loci on a single contig.
   */
  case class SingleContig(map: LociMap.SingleContig[Long]) {

    /** Is the given locus contained in this set? */
    def contains(locus: Long): Boolean = map.contains(locus)

    /** Returns a sequence of ranges giving the intervals of this set. */
    def ranges(): Iterable[SimpleRange] = map.ranges

    /** Number of loci in this set. */
    def count(): Long = map.count

    /** Is this set empty? */
    def isEmpty(): Boolean = map.isEmpty

    /** Iterator through loci in this set, sorted. */
    def individually(): Iterator[Long] = map.lociIndividually()

    /** Returns the union of this set with another. Both must be on the same contig. */
    def union(other: SingleContig): SingleContig = SingleContig(map.union(other.map))

    /** Returns whether a given genomic region overlaps with any loci in this LociSet. */
    def intersects(start: Long, end: Long) = !map.getAll(start, end).isEmpty

    override def toString(): String = truncatedString(Int.MaxValue)

    /** String representation, truncated to maxLength characters. */
    def truncatedString(maxLength: Int = 100): String = map.truncatedString(maxLength, false)
  }
}

// Serialization: just delegate to LociMap[Long].
class LociSetSerializer extends Serializer[LociSet] {
  def write(kryo: Kryo, output: Output, obj: LociSet) = {
    kryo.writeObject(output, obj.map)
  }
  def read(kryo: Kryo, input: Input, klass: Class[LociSet]): LociSet = {
    LociSet(kryo.readObject(input, classOf[LociMap[Long]]))
  }
}

class LociSetSingleContigSerializer extends Serializer[LociSet.SingleContig] {
  def write(kryo: Kryo, output: Output, obj: LociSet.SingleContig) = {
    kryo.writeObject(output, obj.map)
  }
  def read(kryo: Kryo, input: Input, klass: Class[LociSet.SingleContig]): LociSet.SingleContig = {
    LociSet.SingleContig(kryo.readObject(input, classOf[LociMap.SingleContig[Long]]))
  }
}
