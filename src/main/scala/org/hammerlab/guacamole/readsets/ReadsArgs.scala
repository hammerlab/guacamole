package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.logging.DebugLogArgs
import org.kohsuke.args4j.{Option => Args4jOption}

/** Argument for accepting a single set of reads (for non-somatic variant calling). */
trait ReadsArgs extends DebugLogArgs with NoSequenceDictionaryArgs with ReadLoadingConfigArgs {
  @Args4jOption(name = "--reads", metaVar = "X", required = true, usage = "Aligned reads")
  var reads: String = ""
}

