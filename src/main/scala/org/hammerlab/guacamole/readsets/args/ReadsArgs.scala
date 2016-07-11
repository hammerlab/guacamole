package org.hammerlab.guacamole.readsets.args

import org.hammerlab.guacamole.logging.DebugLogArgs
import org.hammerlab.guacamole.readsets.loading.ReadLoadingConfigArgs
import org.kohsuke.args4j.{Option => Args4jOption}

trait ReadsArgsI {
  def reads: String
}

/** Argument for accepting a single set of reads (for non-somatic variant calling). */
trait ReadsArgs extends DebugLogArgs with NoSequenceDictionaryArgs with ReadLoadingConfigArgs with ReadsArgsI {
  @Args4jOption(name = "--reads", metaVar = "X", required = true, usage = "Aligned reads")
  var reads: String = ""
}

