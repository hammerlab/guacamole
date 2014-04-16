package org.bdgenomics.guacamole

import com.google.common.collect.{ImmutableRangeSet, RangeSet, Range}
import scala.collection.immutable.NumericRange
import scala.collection.JavaConverters._
import scala.collection.JavaConversions
import org.bdgenomics.guacamole.LociRanges.{JLong, LongRangeSet, emptyRangeSet}

case class LociRanges(ranges: Map[String, LongRangeSet]) {

  lazy val contigs: Seq[String] = ranges.filter(!_._2.isEmpty).keys.toSeq

  def at(contig: String): Seq[NumericRange[Long]] = ranges.get(contig) match {
    case Some(rangeSet) => JavaConversions.asScalaIterator(rangeSet.asRanges.iterator).map(raw =>
        NumericRange[Long](raw.lowerEndpoint, raw.upperEndpoint, 1)).toSeq.sortBy(_.start)
    case None => Seq.empty
  }

  def combine(other: LociRanges): LociRanges = {
    val keys = (ranges ++ other.ranges).keys
    val pairs = keys.map(key => {
      key ->
        ImmutableRangeSet.builder[JLong]()
          .addAll(ranges.getOrElse(key, emptyRangeSet))
          .addAll(other.ranges.getOrElse(key, emptyRangeSet))
          .build
    })
    LociRanges(pairs.toMap)
  }

  def contains(contig: String, position: Long): Boolean = {
    ranges.get(contig).exists(_.contains(position))
  }

  def intersects(contig: String, start: Long, end: Long) = {
    val range = Range.closedOpen[JLong](start, end)
    ranges.get(contig).exists(!_.subRangeSet(range).isEmpty)
  }
}
object LociRanges {
  private type JLong = java.lang.Long
  private type LongRange = Range[JLong]
  private type LongRangeSet = ImmutableRangeSet[JLong]
  private val emptyRangeSet = ImmutableRangeSet.of[JLong]()
  private def newRangeSet(range: LongRange) = ImmutableRangeSet.of[JLong](range)

  def apply(contigStartEnd: Seq[(String, Long, Long)]): LociRanges = {
    LociRanges(contigStartEnd.map({
      case (contig, start, end) => (contig, ImmutableRangeSet.of(Range.closedOpen[JLong](start, end)))
    }).toMap)
  }

  def parse(loci: String): LociRanges = {
    val syntax = "([A-z]+):([0-9]+)-([0-9]+)".r
    loci.split(',').map({
      case syntax(name, start, end) => LociRanges(Seq[(String, Long, Long)]((name, start.toLong, end.toLong)))
      case other => throw new IllegalArgumentException("Couldn't parse loci range: %s".format(other))
    }).reduce(_.combine(_))
  }
}
