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

package org.hammerlab.guacamole

import com.esotericsoftware.kryo.{ Serializer, Kryo }
import com.esotericsoftware.kryo.io.{ Input, Output }
import org.hammerlab.guacamole.LociMap.SimpleRange

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
  lazy val contigs: Seq[String] = map.contigs

  /** The number of loci in this LociSet. */
  lazy val count: Long = map.count

  /** Does count == 0? */
  lazy val isEmpty = map.isEmpty

  /** Given a contig name, returns a [[LociSet.SingleContig]] giving the loci on that contig. */
  def onContig(contig: String): LociSet.SingleContig = LociSet.SingleContig(map.onContig(contig))

  /** Returns the union of this LociSet with another. */
  def union(other: LociSet): LociSet = LociSet(map.union(other.map))

  /** Returns a string representation of this LociSet, in the same format that LociSet.parse expects. */
  override def toString(): String = truncatedString(Int.MaxValue)

  /** String representation, truncated to maxLength characters. */
  def truncatedString(maxLength: Int = 200): String = map.truncatedString(maxLength, false)

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
   * Split the LociSet into two sets, where the first one has `numToTake` elements, and the second one has the
   * remaining elements.
   *
   * @param numToTake number of elements to take. Must be <= number of elements in the set.
   *
   */
  def take(numToTake: Long): (LociSet, LociSet) = {
    assume(numToTake <= count, "Can't take %d loci from a set of size %d.".format(numToTake, count))

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
     * Add an entire other LociSet to the LociSet under construction.
     */
    def put(lociSet: LociSet): Builder = {
      wrapped.put(lociSet, 0)
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
   *             contig name, and START and END are integers. Whitespace is ignored.
   */
  def parse(loci: String, contigLengths: Option[Map[String, Long]] = None): LociSet = {
    def maybeCheckContigIsValid(contig: String): Unit = contigLengths match {
      case Some(map) if (!map.contains(contig)) =>
        throw new IllegalArgumentException("No such contig: '%s'.".format(contig))
      case _ => {}
    }

    if (loci == "all") {
      contigLengths match {
        case None => throw new IllegalArgumentException("Specifying loci 'all' requires providing contigLengths")
        case Some(map) => {
          val builder = LociSet.newBuilder
          map.foreach(contigNameAndLength => builder.put(contigNameAndLength._1, 0L, contigNameAndLength._2))
          builder.result
        }
      }
    } else {
      val contigAndLoci = """^([\pL\pN._]+):(\pN+)-(\pN+)$""".r
      val contigOnly = """^([\pL\pN._]+)""".r
      val sets = loci.replaceAll("\\s", "").split(',').map({
        case "" => LociSet.empty
        case contigAndLoci(name, start, end) => {
          maybeCheckContigIsValid(name)
          LociSet(name, start.toLong, end.toLong)
        }
        case contigOnly(contig) => contigLengths match {
          case None =>
            throw new IllegalArgumentException(
              "Specifying a contig ('%s') without a loci range requires providing contigLengths".format(contig))
          case Some(map) => {
            maybeCheckContigIsValid(contig)
            LociSet(contig, 0L, map(contig))
          }
        }
        case other =>
          throw new IllegalArgumentException("Couldn't parse loci range: %s".format(other))
      })
      union(sets: _*)
    }
  }

  /** Returns union of specified [[LociSet]] instances. */
  def union(lociSets: LociSet*): LociSet = {
    val result = LociSet.newBuilder
    lociSets.foreach(lociSet => {
      result.put(lociSet)
    })
    result.result
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
    def iterator(): SingleContig.Iterator = new SingleContig.Iterator(this)

    /** Returns the union of this set with another. Both must be on the same contig. */
    def union(other: SingleContig): SingleContig = SingleContig(map.union(other.map))

    /** Returns whether a given genomic region overlaps with any loci in this LociSet. */
    def intersects(start: Long, end: Long) = !map.getAll(start, end).isEmpty

    override def toString(): String = truncatedString(Int.MaxValue)

    /** String representation, truncated to maxLength characters. */
    def truncatedString(maxLength: Int = 100): String = map.truncatedString(maxLength, false)
  }
  object SingleContig {

    /**
     * An iterator over loci on a single contig. Loci from this iterator are sorted (monotonically increasing).
     *
     * This can be used as a plain scala Iterator[Long], but also supports extra functionality for quickly skipping
     * ahead past a given locus.
     *
     * @param loci loci to iterate over
     */
    class Iterator(loci: SingleContig) extends scala.BufferedIterator[Long] {
      private var ranges = loci.ranges.iterator

      /** The range for the next locus to be returned. */
      private var headRangeOption: Option[SimpleRange] = if (ranges.isEmpty) None else Some(ranges.next())

      /** The index in the range for the next locus. */
      private var headIndex = 0L

      /** true if calling next() will succeed. */
      def hasNext() = headRangeOption.nonEmpty

      /** The next element to be returned by next(). If the iterator is empty, throws NoSuchElementException. */
      def head: Long = headRangeOption match {
        case Some(range) => range.start + headIndex
        case None        => throw new NoSuchElementException("empty iterator")
      }

      /**
       * Advance the iterator and return the current head.
       *
       * Throws NoSuchElementException if the iterator is already at the end.
       */
      def next(): Long = {
        val nextLocus: Long = head // may throw

        // Advance
        headIndex += 1
        if (headIndex == headRangeOption.get.length) {
          nextRange()
        }
        nextLocus
      }

      /**
       * Skip ahead to the first locus in the iterator that is at or past the given locus.
       *
       * After calling this, a subsequent call to next() will return the first element in the iterator that is >= the
       * given locus. If there is no such element, then the iterator will be empty after calling this method.
       *
       */
      def skipTo(locus: Long): Unit = {
        // Skip entire ranges until we hit one whose end is past the target locus.
        while (headRangeOption.exists(_.end <= locus)) {
          nextRange()
        }
        // If we're not at the end of the iterator and the current head range includes the target locus, set our index
        // so that our next locus is the target.
        headRangeOption match {
          case Some(range) if (locus >= range.start && locus < range.end) => {
            headIndex = locus - range.start
          }
          case _ => {}
        }
      }

      /**
       * Advance past all loci in the current head range. Return the next range if one exists.
       */
      private def nextRange(): Option[SimpleRange] = {
        headIndex = 0
        headRangeOption = if (ranges.hasNext) Some(ranges.next()) else None
        headRangeOption
      }
    }
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
