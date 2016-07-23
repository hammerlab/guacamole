package org.hammerlab.guacamole.loci.set

import htsjdk.variant.vcf.VCFFileReader
import org.hammerlab.guacamole.readsets.ContigLengths
import org.hammerlab.guacamole.reference.{ContigName, Locus, NumLoci}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Class for constructing a LociSet from string representations of genomic ranges.
 *
 * A LociSet always has an exact size, but a LociParser supports specifications of loci sets before the contigs
 * and their lengths are known. For example, a caller can specify "all sites on all contigs", or "all sites
 * on chromosomes 1 and 2". To build such an "unresolved" LociSet, the contigs and their lengths must be provided to
 * the result method.
 *
 * This comes in handy when we want to pass a specification of loci to BAM reading methods.
 * We don't know the contigs and their lengths until we read in the BAM header, so we can't make a LociSet until
 * we read the file header. At the same time, we want to use the BAM index to read only the loci of interest from
 * the file. A LociParser is a convenient object to pass to the bam loading functions, as it is an object
 * that specifies the loci of interest without requiring us to already know the contigs and their lengths.
 */
class LociParser {
  /**
   * Does this LociParser contain only loci ranges with exact ends (e.g. "chr:1-20000" not "all of chr1")?
   * If false, we require contig lengths to be specified to the result method.
   */
  private var fullyResolved = true

  /**
   * Does this LociParser contain all sites on all contigs?
   */
  private var containsAll = false

  /**
   * (contig, start, end) ranges which have been added to this builder.
   * If end is None, it indicates "until the end of the contig"
   */
  private val ranges = ArrayBuffer[(ContigName, Locus, Option[Locus])]()

  /**
   * Add an interval to this LociParser.
   */
  def put(contigName: ContigName, start: Locus, end: Locus): LociParser = put(contigName, start, Some(end))

  /**
   * Parse a (string) loci expression and add it to the builder. Example expressions:
   *
   *  "all": all sites on all contigs.
   *  "none": no loci, used as a default in some places.
   *  "chr1,chr3": all sites on contigs chr1 and chr3.
   *  "chr1:10000-20000,chr2": sites x where 10000 <= x < 20000 on chr1, all sites on chr2.
   *  "chr1:10000": just chr1, position 10000; equivalent to "chr1:10000-10001".
   *  "chr1:10000-": chr1, from position 10000 to the end of chr1.
   */
  def put(lociStr: String): LociParser = {
    if (lociStr == "all") {
      LociParser.all
    } else if (lociStr == "none") {
      new LociParser()
    } else {
      val contigAndLoci = """^([\pL\pN._]+):(\pN+)(?:-(\pN*))?$""".r
      val contigOnly = """^([\pL\pN._]+)""".r
      lociStr.replaceAll("\\s", "").split(',').foreach {
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
          put(contig, 0, None)
        case other => {
          throw new IllegalArgumentException("Couldn't parse loci range: %s".format(other))
        }
      }
      this
    }
  }

  private def put(contig: ContigName, start: Locus = 0, end: Option[Locus] = None): LociParser = {
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
   * Build the result.
   *
   * The wrappers here all delegate to the private implementation that follows.
   */
  def result: LociSet = result(None)  // enables omitting parentheses: builder.result instead of builder.result()
  def result(contigLengths: ContigLengths): LociSet = result(Some(contigLengths))

  private def result(contigLengthsOpt: Option[ContigLengths] = None): LociSet = {

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

    val resolvedRanges =
      if (containsAll)
        for {
          (contig, length) <- contigLengthsOpt.get
        } yield
          (contig, 0L, length)
      else
        for {
          (contig, start, end) <- ranges
          resolvedEnd = end.getOrElse(contigLengthsOpt.get(contig))
        } yield
          (contig, start, resolvedEnd)

    LociSet(resolvedRanges)
  }
}

object LociParser {
  val all = new LociParser()
  all.containsAll = true
  all.fullyResolved = false

  def apply(lociStr: String): LociParser = new LociParser().put(lociStr)

  def apply(reader: VCFFileReader): LociParser = {
    val loci = new LociParser()
    reader
      .foreach(variant =>
        loci.put(
          variant.getContig,
          variant.getStart - 1L,
          variant.getEnd.toLong
        )
      )
    loci
  }
}
