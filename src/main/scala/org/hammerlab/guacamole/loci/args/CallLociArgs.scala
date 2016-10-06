package org.hammerlab.guacamole.loci.args

import org.kohsuke.args4j.{Option => Args4jOption}

/** Arguments for accepting a set of loci to restrict variant-calling to. */
trait CallLociArgs {
  @Args4jOption(
    name = "--loci",
    usage = "Loci at which to call variants. Either 'all' or contig:start-end,contig:start-end,...",
    forbids = Array("--loci-file")
  )
  protected var lociStr: String = null

  def lociStrOpt: Option[String] = Option(lociStr)

  @Args4jOption(
    name = "--loci-file",
    usage = "Path to file giving loci at which to call variants.",
    forbids = Array("--loci")
  )
  protected var lociFile: String = null

  def lociFileOpt: Option[String] = Option(lociFile)
}
