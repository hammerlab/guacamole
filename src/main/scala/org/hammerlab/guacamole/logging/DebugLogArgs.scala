package org.hammerlab.guacamole.logging

import org.bdgenomics.utils.cli.ParquetArgs
import org.hammerlab.guacamole.commands.Args
import org.kohsuke.args4j.{Option => Args4jOption}

trait DebugLogArgs extends Args {
  @Args4jOption(name = "--debug", usage = "If set, prints a higher level of debug output.")
  var debug = false
}
