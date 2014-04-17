package org.bdgenomics.guacamole

import com.google.common.collect.{ TreeRangeSet, ImmutableRangeSet, RangeSet, Range }
import scala.collection.immutable.NumericRange
import scala.collection.JavaConverters._
import scala.collection.JavaConversions
import org.bdgenomics.guacamole.LociSet.{ SingleContig, JLong, emptyRangeSet }

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
case class LociSet(ranges: Map[String, SingleContig]) {

  /** The contigs included in this LociSet with a nonempty set of loci. */
  lazy val contigs: Seq[String] = ranges.filter(!_._2.isEmpty).keys.toSeq.sorted

  /**
   * Returns the loci on the specified contig.
   *
   * @param contig The contig name
   * @return A [[LociSet.SingleContig]] instance giving the loci on the specified contig.
   */
  def onContig(contig: String): SingleContig = ranges.get(contig) match {
    case Some(singleContigLociSet) => singleContigLociSet
    case None                      => SingleContig(contig, emptyRangeSet)
  }

  /** Returns the union of this LociSet with another. */
  def union(other: LociSet): LociSet = {
    val keys = (ranges ++ other.ranges).keys
    val pairs = keys.map(key => key -> onContig(key).union(other.onContig(key)))
    LociSet(pairs.toMap)
  }

  /** Returns a string representation of this LociSet, in the same format that LociSet.parse expects. */
  override def toString(): String = contigs.flatMap(_.toString).mkString(",")
}
object LociSet {
  private type JLong = java.lang.Long
  private val emptyRangeSet = ImmutableRangeSet.of[JLong]()

  /** An empty LociSet. */
  val empty = LociSet(Map[String, SingleContig]())

  /** Return a LociSet of a single genomic interval. */
  def apply(contig: String, start: Long, end: Long): LociSet = {
    LociSet(Map[String, SingleContig](
      contig -> SingleContig(contig, ImmutableRangeSet.of(Range.closedOpen[JLong](start, end)))))
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
    val syntax = """^([\pL\pN]+):(\pN+)-(\pN+)""".r
    //val syntax = "([A-z]+):([0-9]+)-([0-9]+)".r
    val sets = loci.replace(" ", "").split(',').map({
      case syntax(name, start, end) => LociSet(Seq[(String, Long, Long)]((name, start.toLong, end.toLong)))
      case other                    => throw new IllegalArgumentException("Couldn't parse loci range: %s".format(other))
    })
    sets.reduce(_.union(_))
  }

  /**
   * A set of loci on a single contig.
   * @param contig The contig name
   * @param rangeSet The range set of loci on this contig.
   */
  case class SingleContig(contig: String, rangeSet: RangeSet[JLong]) {

    /** Is the given locus contained in this set? */
    def contains(locus: Long): Boolean = rangeSet.contains(locus)

    /** Returns a sequence of ranges giving the intervals of this set. */
    lazy val ranges: Seq[NumericRange[Long]] =
      rawIterator.map(raw => NumericRange[Long](raw.lowerEndpoint, raw.upperEndpoint, 1)).toSeq.sortBy(_.start)

    /** Number of loci in this set. */
    lazy val count: Long = rawIterator.map(raw => raw.upperEndpoint - raw.lowerEndpoint).sum

    /** Is this set empty? */
    lazy val isEmpty: Boolean = count == 0

    /** Iterator through loci in this set, sorted. */
    def individually(): Iterator[Long] = ranges.iterator.flatMap(_.iterator)

    /** Returns the union of this set with another. Both must be on the same contig. */
    def union(other: SingleContig): SingleContig = {
      assume(contig == other.contig)
      val both = TreeRangeSet.create[JLong]()
      both.addAll(rangeSet)
      both.addAll(other.rangeSet)
      SingleContig(contig, both)
    }

    /** Returns whether a given genomic region overlaps with any loci in this LociSet. */
    def intersects(start: Long, end: Long) = {
      val range = Range.closedOpen[JLong](start, end)
      rangeSet.subRangeSet(range).isEmpty
    }

    override def toString(): String = {
      ranges.map(range => "%s:%l-%l".format(contig, range.start, range.end)).mkString(",")
    }

    private def rawIterator() = JavaConversions.asScalaIterator(rangeSet.asRanges.iterator)
  }
}
