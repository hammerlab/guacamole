package org.hammerlab.guacamole.loci

import java.io.InputStreamReader

import org.apache.commons.io.IOUtils
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
    val lociToParse =
      if (loci.nonEmpty) {
        loci
      } else if (lociFile.nonEmpty) {
        // Load loci from file.
        val path = new Path(lociFile)
        val filesystem = path.getFileSystem(hadoopConfiguration)
        IOUtils.toString(new InputStreamReader(filesystem.open(path)))
      } else {
        fallback
      }

    LociParser(lociToParse)
  }
}
