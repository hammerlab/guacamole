package org.hammerlab.guacamole.loci.set

import java.lang.{Long => JLong}

import com.google.common.collect.{TreeRangeSet, Range => JRange}

import scala.collection.immutable.TreeMap
import com.google.common.collect.{TreeRangeSet, Range => JRange}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import java.lang.{Long => JLong}

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
 * the file. A Builder is a convenient object to pass to the bam loading functions, as it is an object
 * that specifies the loci of interest without requiring us to already know the contigs and their lengths.
 */
class Builder {
  /**
   * Does this Builder contain only loci ranges with exact ends (e.g. "chr:1-20000" not "all of chr1")?
   * If false, we require contig lengths to be specified to the result method.
   */
  private var fullyResolved = true

  /**
   * Does this Builder contain all sites on all contigs?
   */
  private var containsAll = false

  /**
   * (contig, start, end) ranges which have been added to this builder.
   * If end is None, it indicates "until the end of the contig"
   */
  private val ranges = ArrayBuffer[(String, Long, Option[Long])]()

  /**
   * Add an interval to the Builder.
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
      Builder.all
    } else if (loci == "none") {
      new Builder()
    } else {
      val contigAndLoci = """^([\pL\pN._]+):(\pN+)(?:-(\pN*))?$""".r
      val contigOnly = """^([\pL\pN._]+)""".r
      loci.replaceAll("\\s", "").split(',').foreach {
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
      }
      this
    }
  }

  /**
   * Build the result.
   *
   * The wrappers here all delegate to the private implementation that follows.
   */
  def result: LociSet = result(None)  // enables omitting parentheses: builder.result instead of builder.result()
  def result(contigLengths: Map[String, Long]): LociSet = result(Some(contigLengths))
  def result(contigLengths: (String, Long)*): LociSet =
    result(
      // Calling .result() should pass None, not Some(Map.empty)
      if (contigLengths.nonEmpty)
        Some(contigLengths.toMap)
      else
        None
    )

  private def result(contigLengthsOpt: Option[Map[String, Long]] = None): LociSet = {

    // Check for invalid contigs.
    for {
      contigLengths <- contigLengthsOpt.toList
      (contig, start, end) <- ranges
    } {
      contigLengths.get(contig) match {
        case None =>
          throw new IllegalArgumentException(
            s"No such contig: $contig. Valid contigs: ${contigLengths.keys.mkString(", ")}"
          )
        case Some(contigLength) if end.exists(_ > contigLength) =>
          throw new IllegalArgumentException(
            s"Invalid range $start-${end.get} for contig '$contig' which has length $contigLength"
          )
        case _ =>
      }
    }

    val rangesByContig = mutable.HashMap[String, TreeRangeSet[JLong]]()

    if (containsAll) {
      for {
        (contig, length) <- contigLengthsOpt.get
      } {
        rangesByContig
          .getOrElseUpdate(contig, TreeRangeSet.create())
          .add(JRange.closedOpen(0L, length))
      }
    } else {
      for {
        (contig, start, end) <- ranges
      } {
        val resolvedEnd = end.getOrElse(contigLengthsOpt.get(contig))
        rangesByContig
          .getOrElseUpdate(contig, TreeRangeSet.create())
          .add(JRange.closedOpen(start, resolvedEnd))
      }
    }

    val mapBuilder = Map.newBuilder[String, Contig]
    for {
      (name, ranges) <- rangesByContig
      if !ranges.isEmpty
    } {
      mapBuilder += name -> Contig(name, ranges)
    }
    LociSet(mapBuilder.result())
  }
}

object Builder {
  val all = new Builder()
  all.containsAll = true
  all.fullyResolved = false

  def apply(lociStr: String): Builder = new Builder().putExpression(lociStr)
}
