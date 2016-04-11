package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.Common.Arguments.Base
import org.kohsuke.args4j.{Option => Args4jOption}

/** Arguments for accepting two sets of reads (tumor + normal). */
trait TumorNormalReadsArgs extends Base with NoSequenceDictionaryArgs with ReadLoadingConfigArgs {
  @Args4jOption(name = "--normal-reads", metaVar = "X", required = true, usage = "Aligned reads: normal")
  var normalReads: String = ""

  @Args4jOption(name = "--tumor-reads", metaVar = "X", required = true, usage = "Aligned reads: tumor")
  var tumorReads: String = ""
}

