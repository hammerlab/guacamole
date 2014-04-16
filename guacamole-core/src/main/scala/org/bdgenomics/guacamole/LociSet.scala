package org.bdgenomics.guacamole

import com.google.common.collect.{ ImmutableRangeSet, RangeSet, Range }
import scala.collection.immutable.NumericRange
import scala.collection.JavaConverters._
import scala.collection.JavaConversions
import org.bdgenomics.guacamole.LociSet.{ JLong, LongRangeSet, emptyRangeSet }

/**
 * A collection of genomic regions. Maps reference names (contig names) to a set of loci on that contig.
 *
 * Used, for example, to keep track of what loci to call variants at.
 *
 * Since contiguous genomic intervals are a common case, this is implemented with sets of (start, end) intervals.
 *
 * All intervals are half open: inclusive on start, exclusive on end.
 *
 * @param ranges Map from contig names to the range set giving the loci under consideration on that contig.
 */
case class LociSet(ranges: Map[String, LongRangeSet]) {

  /** The contigs included in this LociSet: those with a nonempty set of loci. */
  lazy val contigs: Seq[String] = ranges.filter(!_._2.isEmpty).keys.toSeq.sorted

  /**
   * Returns the (start, end) intervals included in a particular contig.
   *
   * @param contig The contig name
   */
  def at(contig: String): Seq[NumericRange[Long]] = ranges.get(contig) match {
    case Some(rangeSet) => JavaConversions.asScalaIterator(rangeSet.asRanges.iterator).map(raw =>
      NumericRange[Long](raw.lowerEndpoint, raw.upperEndpoint, 1)).toSeq.sortBy(_.start)
    case None => Seq.empty
  }

  /** Returns the union of this LociSet with another. */
  def union(other: LociSet): LociSet = {
    val keys = (ranges ++ other.ranges).keys
    val pairs = keys.map(key => {
      key ->
        ImmutableRangeSet.builder[JLong]()
        .addAll(ranges.getOrElse(key, emptyRangeSet))
        .addAll(other.ranges.getOrElse(key, emptyRangeSet))
        .build
    })
    LociSet(pairs.toMap)
  }

  /** Returns whether a given locus on a given contig is included in this LociSet. */
  def contains(contig: String, position: Long): Boolean = {
    ranges.get(contig).exists(_.contains(position))
  }

  /** Returns whether a given genomic region overlaps with any loci in this LociSet. */
  def intersects(contig: String, start: Long, end: Long) = {
    val range = Range.closedOpen[JLong](start, end)
    ranges.get(contig).exists(!_.subRangeSet(range).isEmpty)
  }

  /** Returns a string representation of this LociSet, in the same format that LociSet.parse expects. */
  override def toString(): String = {
    val pieces = for {
      contig <- contigs
      ranges = at(contig)
      range <- ranges
    } yield "%s:%l-%l".format(contig, range.start, range.end)
    pieces.mkString(",")
  }
}
object LociSet {
  private type JLong = java.lang.Long
  private type LongRange = Range[JLong]
  private type LongRangeSet = ImmutableRangeSet[JLong]
  private val emptyRangeSet = ImmutableRangeSet.of[JLong]()
  private def newRangeSet(range: LongRange) = ImmutableRangeSet.of[JLong](range)

  /** Return a LociSet of a single genomic interval. */
  def apply(contig: String, start: Long, end: Long): LociSet = {
    LociSet(Map[String, LongRangeSet](contig -> ImmutableRangeSet.of(Range.closedOpen[JLong](start, end))))
  }

  /**
   * Given a sequence of (contig name, start locus, end locus) triples, returns a LociSet of the specified
   * loci. The intervals supplied are allowed to overlap.
   */
  def apply(contigStartEnd: Seq[(String, Long, Long)]): LociSet = {
    val sets = for ((contig, start, end) <- contigStartEnd) yield LociSet(contig, start, end)
    sets.reduce(_.union(_))
  }

  /**
   * Return a LociSet parsed from a string representation.
   *
   * @param loci A string of the form "CONTIG:START-END,CONTIG:START-END,..." where CONTIG is a string giving the
   *             contig name, and START and END are integers. Spaces are ignored.
   */
  def parse(loci: String): LociSet = {
    val syntax = "([A-z]+):([0-9]+)-([0-9]+)".r
    val sets = loci.replace(" ", "").split(',').map({
      case syntax(name, start, end) => LociSet(Seq[(String, Long, Long)]((name, start.toLong, end.toLong)))
      case other                    => throw new IllegalArgumentException("Couldn't parse loci range: %s".format(other))
    })
    sets.reduce(_.union(_))
  }
}
