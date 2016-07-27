package org.hammerlab.guacamole.loci

import java.io.File

import breeze.io.TextReader.InputStreamReader
import htsjdk.variant.vcf.VCFFileReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.hammerlab.guacamole.loci.set.LociParser
import org.hammerlab.guacamole.logging.DebugLogArgs
import org.kohsuke.args4j.{Option => Args4jOption}

/** Argument for accepting a set of loci. */
trait LociArgs extends DebugLogArgs {
  @Args4jOption(
    name = "--loci",
    usage = "Loci at which to call variants. Either 'all' or contig:start-end,contig:start-end,...",
    forbids = Array("--loci-file")
  )
  var loci: String = ""

  @Args4jOption(
    name = "--loci-file",
    usage = "Path to file giving loci at which to call variants.",
    forbids = Array("--loci")
  )
  var lociFile: String = ""

  def parseLoci(hadoopConfiguration: Configuration, fallback: String = "all"): LociParser =
    LociArgs.parseLoci(loci, lociFile, hadoopConfiguration)
}

object LociArgs {
  /**
   * Parse string representations of loci ranges, either from the "--loci" cmdline parameter or a file specified by the
   * "--loci-file" parameter, and return a LociParser encapsulating the result. The latter can then be converted
   * into a LociSet when contig-lengths are available / have been parsed from read-sets.
   *
   * @param fallback If neither "--loci" nor "--loci-file" were provided, fall back to this string representation
   *                 of the loci that should be considered.
   * @return a LociParser wrapping the appropriate loci ranges.
   */
  def parseLoci(loci: String,
                lociFile: String,
                hadoopConfiguration: Configuration,
                fallback: String = "all"): LociParser = {
    if (loci.nonEmpty && lociFile.nonEmpty) {
      throw new IllegalArgumentException("Specify at most one of the 'loci' and 'loci-file' arguments")
    }

    if (loci.nonEmpty) {
      LociParser(loci)
    } else if (lociFile.nonEmpty) {
      loadFromFile(lociFile, hadoopConfiguration)
    } else {
      LociParser(fallback)
    }
  }

  /**
   * Load a LociSet from the specified file, using the contig lengths from the given ReadSet.
   *
   * @param filePath path to file containing loci. If it ends in '.vcf' then it is read as a VCF and the variant sites
   *                 are the loci. If it ends in '.loci' or '.txt' then it should be a file containing loci as
   *                 "chrX:5-10,chr12-10-20", etc. Whitespace is ignored.
   * @return a LociSet
   */
  private def loadFromFile(filePath: String, hadoopConfiguration: Configuration): LociParser = {
    if (filePath.endsWith(".vcf")) {
      // VCF-reading currently only works for local files, requires "file://" scheme to not be present.
      // TODO: use hadoop-bam to load VCF from local filesystem or HDFS.
      LociParser(
        new VCFFileReader(new File(filePath), false)
      )
    } else if (filePath.endsWith(".loci") || filePath.endsWith(".txt")) {
      val path = new Path(filePath)
      val filesystem = path.getFileSystem(hadoopConfiguration)
      val is = filesystem.open(path)
      val loci =
        LociParser(
          new InputStreamReader(is).readRemaining()
        )
      is.close()
      loci
    } else {
      throw new IllegalArgumentException(
        s"Couldn't guess format for file: $filePath. Expected file extensions: '.loci' or '.txt' for loci string format; '.vcf' for VCFs."
      )
    }
  }
}
