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

package org.hammerlab.guacamole.loci

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import htsjdk.samtools.util.Interval
import org.hammerlab.guacamole.loci.LociMap.SimpleRange

import scala.collection.mutable.ArrayBuffer

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

  /** Given a contig name, returns a [[LociSet.SingleContig]] giving the loci on that contig. */
  def onContig(contig: String): LociSet.SingleContig = LociSet.SingleContig(map.onContig(contig))

  /** Returns the union of this LociSet with another. */
  def union(other: LociSet): LociSet = LociSet(map.union(other.map))

  /** Returns a string representation of this LociSet, in the same format that LociSet.parse expects. */
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

  /**
    * Build a collection of HTSJDK Intervals which are closed [start, end], 1-based intervals
    */
  def toHtsJDKIntervals: List[Interval] = {
    map
      .contigs
      .iterator
      .flatMap(
        contig => {
          map
            .onContig(contig)
            .asMap
            .iterator
            // We had 1 to the start to move to 1-based coordinates
            // Since the `Interval` end is inclusive, we are adding and subtracting 1, no-op
            .map(pieces => new Interval(contig, pieces._1.start.toInt + 1, pieces._1.end.toInt))
        }).toList
  }

  /**
    * String representation of HTSJDK intervals which are closed [start, end], 1-based intervals
    */
  def toHtsJDKIntervalString: String = {
    toHtsJDKIntervals.map(_.toString).mkString(",")
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
   *
   * A LociSet always has an exact size, but a Builder supports specifications of loci sets before the contigs
   * and their lengths are known. For example, a builder can specify "all sites on all contigs", or "all sites
   * on chromosomes 1 and 2". To build such an "unresolved" LociSet, the contigs and their lengths must be provided to
   * the result method.
   *
   * This comes in handy when we want to pass a specification of loci to the BAM reading methods.
   * We don't know the contigs and their lengths until we read in the BAM file, so we can't make a LociSet until
   * we read the file header. At the same time, we want to use the BAM index to read only the loci of interest from
   * the file. A LociSet.Builder is a convenient object to pass to the bam loading functions, as it is an object
   * that specifies the loci of interest without requiring us to already know the contigs and their lengths.
   *
   */
  class Builder {
    /**
     * Does this Builder contain only loci ranges with exact ends (e.g. "chr:1-20000" not "all of chr1")?
     * If false, we require contig lengths to be specified to the result method.
     */
    var fullyResolved = true

    /**
     * Does this Builder contain all sites on all contigs?
     */
    var containsAll = false

    /**
     * (contig, start, end) ranges which have been added to this builder.
     * If end is None, it indicates "until the end of the contig"
     */
    private val ranges = ArrayBuffer.newBuilder[(String, Long, Option[Long])]

    /**
     * Specify that this builder contains all sites on all contigs.
     */
    def putAllContigs(): Builder = {
      containsAll = true
      fullyResolved = false
      this
    }

    /**
     * Add an interval to the Builder. If end is not specified, then it is taken to be the length of the contig.
     */
    def put(contig: String, start: Long, end: Long): Builder = put(contig, start, Some(end))
    def put(contig: String, start: Long = 0, end: Option[Long] = None): Builder = {
      assume(start >= 0)
      assume(end.forall(_ >= start))
      if (!containsAll) {
        ranges += ((contig, start, end))
        if (end.isEmpty) {
          fullyResolved = false
        }
      }
      this
    }

    /**
      * Parse a loci expression and add it to the builder. Example expressions:
      *
      *  "all": all sites on all contigs.
      *  "none": no loci, used as a default in some places.
      *  "chr1,chr3": all sites on contigs chr1 and chr3.
      *  "chr1:10000-20000,chr2": sites x where 10000 <= x < 20000 on chr1, all sites on chr2.
      *  "chr1:10000": just chr1, position 10000; equivalent to "chr1:10000-10001".
      *  "chr1:10000-": chr1, from position 10000 to the end of chr1.
      */
    def putExpression(loci: String): Builder = {
      if (loci == "all") {
        putAllContigs()
      } else if (loci != "none") {
        val contigAndLoci = """^([\pL\pN._]+):(\pN+)(?:-(\pN*))?$""".r
        val contigOnly = """^([\pL\pN._]+)""".r
        loci.replaceAll("\\s", "").split(',').foreach({
          case ""                              => {}
          case contigAndLoci(name, startStr, endStrOpt) =>
            val start = startStr.toLong
            val end = Option(endStrOpt) match {
              case Some("") => None
              case Some(s) => Some(s.toLong)
              case None => Some(start + 1)
            }
            put(name, start, end)
          case contigOnly(contig) =>
            put(contig)
          case other => {
            throw new IllegalArgumentException("Couldn't parse loci range: %s".format(other))
          }
        })
      }
      this
    }

    /**
     * Build the result.
     */
    def result(contigLengths: Option[Map[String, Long]] = None): LociSet = {
      assume(contigLengths.nonEmpty || fullyResolved)
      val wrapped = new LociMap.Builder[Long]
      val rangesResult = ranges.result

      // Check for invalid contigs.
      if (contigLengths.nonEmpty) {
        rangesResult.foreach({
          case (contig, start, end) => contigLengths.get.get(contig) match {
            case None => throw new IllegalArgumentException(
              "No such contig: %s. Valid contigs: %s".format(contig, contigLengths.get.keys.mkString(", ")))
            case Some(contigLength) if end.exists(_ > contigLength) =>
              throw new IllegalArgumentException(
                "Invalid range %d-%d for contig '%s' which has length %d".format(
                  start, end.get, contig, contigLength))
            case _ => {}
          }
        })
      }
      if (containsAll) {
        contigLengths.get.foreach(
          contigAndLength => wrapped.put(contigAndLength._1, 0, contigAndLength._2 - 1, 0))
      } else {
        rangesResult.foreach({
          case (contig, start, end) => {
            val resolvedEnd = end.getOrElse(contigLengths.get.apply(contig))
            wrapped.put(contig, start, resolvedEnd, 0)
          }
        })
      }
      LociSet(wrapped.result)
    }

    /* Convenience wrappers. */
    def result: LociSet = result(None) // enables omitting parentheses: builder.result instead of builder.result()
    def result(contigLengths: Map[String, Long]): LociSet = result(Some(contigLengths))
    def result(contigLengths: (String, Long)*): LociSet =
      result(
        if (contigLengths.nonEmpty)
          Some(contigLengths.toMap)
        else
          None
      )
  }

  /** Return a LociSet of a single genomic interval. */
  def apply(contig: String, start: Long, end: Long): LociSet = {
    (new Builder).put(contig, start, end).result
  }

  /**
   * Return a LociSet.Builder parsed from a string representation.
   * See LociSet.Builder.putExpression for example expressions.
   */
  def parse(loci: String): LociSet.Builder = {
    LociSet.newBuilder.putExpression(loci)
  }

  /** Returns union of specified [[LociSet]] instances. */
  def union(lociSets: LociSet*): LociSet = {
    val wrapped = LociMap.newBuilder[Long]
    lociSets.foreach(lociSet => {
      wrapped.put(lociSet, 0)
    })
    LociSet(wrapped.result)
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
    def isEmpty: Boolean = map.isEmpty

    /** Iterator through loci in this set, sorted. */
    def iterator(): SingleContig.Iterator = new SingleContig.Iterator(this)

    /** Returns the union of this set with another. Both must be on the same contig. */
    def union(other: SingleContig): SingleContig = SingleContig(map.union(other.map))

    /** Returns whether a given genomic region overlaps with any loci in this LociSet. */
    def intersects(start: Long, end: Long) = map.getAll(start, end).nonEmpty

    override def toString: String = truncatedString(Int.MaxValue)

    /** String representation, truncated to maxLength characters. */
    def truncatedString(maxLength: Int = 100): String = map.truncatedString(maxLength, includeValues = false)
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
      private val ranges = loci.ranges.iterator

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
