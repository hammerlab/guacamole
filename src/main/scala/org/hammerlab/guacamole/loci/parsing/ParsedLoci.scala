package org.hammerlab.guacamole.loci.parsing

import java.io.File

import htsjdk.variant.vcf.VCFFileReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.readsets.ContigLengths
import org.hammerlab.guacamole.variants.VariantContext

import scala.collection.JavaConversions._
import scala.io.Source

/**
 * Class representing genomic ranges that have been parsed from various supported [[String]]-representations.
 *
 * Ranges can be a sequence of (possibly open-ended) loci-ranges on various contigs, or a special [[All]] value denoting
 * all loci on all contigs; see [[ParsedLociRanges]] for more info.
 *
 * Typically, a [[ParsedLoci]] is a short-lived intermediate representation between [[String]] representations of
 * loci-ranges (that may be passed in cmdline-flags or read in from a file) and a [[LociSet]], which can only be
 * instantiated once contig-lengths are known (e.g. read from a BAM header).
 *
 * [[ParsedLoci]]'s [[result]] method is a common way for turning parsed loci and contig-lengths into a [[LociSet]].
 */
class ParsedLoci private(ranges: ParsedLociRanges) {
  /**
   * Build a LociSet from parsed loci, using contig lengths to resolve open-ended ranges.
   */
  def result(contigLengths: ContigLengths): LociSet = result(Some(contigLengths))

  private def result(contigLengthsOpt: Option[ContigLengths] = None): LociSet =
    LociSet(
      ranges match {
        case All =>
          for {
            (contig, length) <- contigLengthsOpt.get
          } yield
            (contig, 0L, length)
        case LociRanges(ranges) =>
          val contigLengths = contigLengthsOpt.getOrElse(Map.empty)
          for {
            LociRange(contigName, start, endOpt) <- ranges
            contigLengthOpt = contigLengths.get(contigName)
          } yield
            (endOpt, contigLengthOpt) match {
              case (Some(end), Some(contigLength)) if end > contigLength =>
                throw new IllegalArgumentException(
                  s"Invalid range $start-${endOpt.get} for contig '$contigName' which has length $contigLength"
                )
              case (Some(end), _) =>
                (contigName, start, end)
              case (_, Some(contigLength)) =>
                (contigName, start, contigLength)
              case _ =>
                throw new IllegalArgumentException(
                  s"No such contig: $contigName. Valid contigs: ${contigLengths.keys.mkString(", ")}"
                )
            }
      }
    )

  // NOTE(ryan): only used in tests; TODO: move to test-specific helper.
  def result: LociSet = result(None)
}

object ParsedLoci {
  val all = new ParsedLoci(All)

  val empty = new ParsedLoci(LociRanges(Nil))

  /**
   * Parse string representations of loci ranges, either from one string (lociOpt) or a file with one range per line
   * (lociFileOpt), and return a [[ParsedLoci]] encapsulating the result. The latter can then be converted into a
   * [[org.hammerlab.guacamole.loci.set.LociSet]] when contig-lengths are available / have been parsed from read-sets.
   */
  def fromArgs(lociStrOpt: Option[String],
               lociFileOpt: Option[String],
               hadoopConfiguration: Configuration): Option[ParsedLoci] =
    (lociStrOpt, lociFileOpt) match {
      case (Some(lociStr), _) => Some(ParsedLoci(lociStr))
      case (_, Some(lociFile)) => Some(loadFromFile(lociFile, hadoopConfiguration))
      case _ =>
        None
    }

  /**
   * Load a LociSet from the specified file, using the contig lengths from the given ReadSet.
   *
   * @param lociFile path to file containing loci. If it ends in '.vcf' then it is read as a VCF and the variant sites
   *                 are the loci. If it ends in '.loci' or '.txt' then it should be a file containing loci as
   *                 "chrX:5-10,chr12-10-20", etc. Whitespace is ignored.
   * @return a LociSet
   */
  private def loadFromFile(lociFile: String, hadoopConfiguration: Configuration): ParsedLoci =
    if (lociFile.endsWith(".vcf")) {
      fromVCF(lociFile)
    } else if (lociFile.endsWith(".loci") || lociFile.endsWith(".txt")) {
      val path = new Path(lociFile)
      val filesystem = path.getFileSystem(hadoopConfiguration)
      val is = filesystem.open(path)
      val lines = Source.fromInputStream(is).getLines()
      ParsedLoci(lines)
    } else
      throw new IllegalArgumentException(
        s"Couldn't guess format for file: $lociFile. Expected file extensions: '.loci' or '.txt' for loci string format; '.vcf' for VCFs."
      )

  def apply(lociStrs: String): ParsedLoci = apply(Iterator(lociStrs))

  def apply(lociStrs: Iterator[String]): ParsedLoci =
    new ParsedLoci(ParsedLociRanges(lociStrs))

  def apply(range: LociRange): ParsedLoci = apply(Iterable(range))

  def apply(ranges: Iterable[LociRange]): ParsedLoci =
    new ParsedLoci(LociRanges(ranges))

  def fromVCF(lociFile: String): ParsedLoci =
    apply(
      // VCF-reading currently only works for local files, requires "file://" scheme to not be present.
      // TODO: use hadoop-bam to load VCF from local filesystem or HDFS.
      new VCFFileReader(new File(lociFile), false)
        .map {
          case VariantContext(contigName, start, end) =>
            LociRange(contigName, start, end)
        }
    )
}
