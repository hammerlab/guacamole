package org.hammerlab.guacamole.loci.args

import org.kohsuke.args4j.{Option => Args4jOption}

trait ForceCallLociArgs {
  @Args4jOption(
    name = "--force-call-loci",
    usage = "Always call the given sites"
  )
  protected var forceCallLociStr: String = null

  def forceCallLociStrOpt: Option[String] = Option(forceCallLociStr)

  @Args4jOption(
    name = "--force-call-loci-file",
    usage = "Always call the given sites"
  )
  protected var forceCallLociFile: String = null

  def forceCallLociFileOpt: Option[String] = Option(forceCallLociFile)
}
