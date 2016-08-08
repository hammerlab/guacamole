package org.hammerlab.guacamole.readsets.args

import org.hammerlab.guacamole.logging.DebugLogArgs
import org.kohsuke.args4j.{Option => Args4jOption}

/** Argument for using / not using sequence dictionaries to get contigs and lengths. */
trait NoSequenceDictionaryArgs {
  @Args4jOption(name = "--no-sequence-dictionary",
    usage = "If set, get contigs and lengths directly from reads instead of from sequence dictionary.")
  var noSequenceDictionary: Boolean = false
}

