package org.hammerlab.guacamole.readsets.args

import org.hammerlab.guacamole.logging.DebugLogArgs
import org.hammerlab.guacamole.readsets.loading.ReadLoadingConfigArgs
import org.kohsuke.args4j.{Option => Args4jOption}

/** Arguments for accepting two sets of reads (tumor + normal). */
trait TumorNormalReadsArgs extends DebugLogArgs with NoSequenceDictionaryArgs with ReadLoadingConfigArgs {
  @Args4jOption(name = "--normal-reads", metaVar = "X", required = true, usage = "Aligned reads: normal")
  var normalReads: String = ""

  @Args4jOption(name = "--tumor-reads", metaVar = "X", required = true, usage = "Aligned reads: tumor")
  var tumorReads: String = ""
}

