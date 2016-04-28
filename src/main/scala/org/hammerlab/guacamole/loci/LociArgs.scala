package org.hammerlab.guacamole.loci

import java.io.InputStreamReader

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.hammerlab.guacamole.loci.set.LociParser
import org.hammerlab.guacamole.logging.DebugLogArgs
import org.kohsuke.args4j.{Option => Args4jOption}

/** Argument for accepting a set of loci. */
trait LociArgs extends DebugLogArgs {
  @Args4jOption(
    name = "--loci",
    usage = "Loci at which to call variants. Either 'all' or contig:start-end,contig:start-end,...",
    forbids = Array("--loci-from-file")
  )
  var loci: String = ""

  @Args4jOption(
    name = "--loci-from-file",
    usage = "Path to file giving loci at which to call variants.",
    forbids = Array("--loci")
  )
  var lociFromFile: String = ""

  /**
   * Parse string representations of loci ranges, either from the "--loci" cmdline parameter or a file specified by the
   * "--loci-from-file" parameter, and return a LociParser encapsulating the result. The latter can then be converted
   * into a LociSet when contig-lengths are available / have been parsed from read-sets.
   *
   * @param fallback If neither "--loci" nor "--loci-from-file" were provided, fall back to this string representation
   *                 of the loci that should be considered.
   * @return a LociParser wrapping the appropriate loci ranges.
   */
  def parseLoci(fallback: String = "all"): LociParser = {
    if (loci.nonEmpty && lociFromFile.nonEmpty) {
      throw new IllegalArgumentException("Specify at most one of the 'loci' and 'loci-from-file' arguments")
    }
    val lociToParse =
      if (loci.nonEmpty) {
        loci
      } else if (lociFromFile.nonEmpty) {
        // Load loci from file.
        val filesystem = FileSystem.get(new Configuration())
        val path = new Path(lociFromFile)
        IOUtils.toString(new InputStreamReader(filesystem.open(path)))
      } else {
        fallback
      }

    LociParser(lociToParse)
  }
}

