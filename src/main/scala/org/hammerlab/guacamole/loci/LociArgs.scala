package org.hammerlab.guacamole.loci

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
}
