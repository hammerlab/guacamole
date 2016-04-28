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

  def parse(default: String = "all"): LociParser = {
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
        default
      }

    LociParser(lociToParse)
  }
}

