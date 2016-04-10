package org.hammerlab.guacamole.logging

import org.bdgenomics.utils.cli.{Args4jBase, ParquetArgs}
import org.kohsuke.args4j.{Option => Args4jOption}

trait DebugLogArgs extends Args4jBase with ParquetArgs {
  @Args4jOption(name = "--debug", usage = "If set, prints a higher level of debug output.")
  var debug = false
}

