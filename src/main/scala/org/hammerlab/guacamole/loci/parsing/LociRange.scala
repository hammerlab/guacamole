package org.hammerlab.guacamole.loci.parsing

import org.hammerlab.guacamole.reference.{ContigName, Locus}

/**
 * Representation of a genomic range as parsed from a cmdline-flag or file.
 *
 * Example ranges:
 *
 *   - chr2:10-20
 *   - chr3
 *   - chr4:100-
 *   - all
 *   - none
 */
sealed trait ParsedLociRange

object ParsedLociRange {

  val contigAndLoci = """^([\pL\pN._]+):(\pN+)(?:-(\pN*))?$""".r
  val contigOnly = """^([\pL\pN._]+)""".r

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
  def apply(lociRangeStr: String): Option[ParsedLociRange] =
    lociRangeStr.replaceAll("\\s", "") match {
      case "all" =>
        Some(AllRange)
      case "none" | "" =>
        None
      case contigAndLoci(name, startStr, endStrOpt) =>
        val start = startStr.toLong
        val endOpt = Option(endStrOpt) match {
          case Some("") => None
          case Some(s) => Some(s.toLong)
          case None => Some(start + 1)
        }
        Some(LociRange(name, start, endOpt))
      case contigOnly(contig) =>
        Some(LociRange(contig, 0, None))
      case other => {
        throw new IllegalArgumentException(s"Couldn't parse loci range: $other")
      }
    }
}

case object AllRange extends ParsedLociRange

case class LociRange(contigName: ContigName,
                     start: Locus,
                     endOpt: Option[Locus])
  extends ParsedLociRange

object LociRange {
  def apply(contigName: ContigName, start: Locus, end: Locus): LociRange =
    LociRange(contigName, start, Some(end))

  def apply(tuple: (ContigName, Locus, Locus)): LociRange = {
    val (contigName, start, end) = tuple
    LociRange(contigName, start, Some(end))
  }
}
