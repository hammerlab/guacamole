package org.hammerlab.guacamole.reference

import org.hammerlab.guacamole.logging.DebugLogArgs
import org.kohsuke.args4j.{Option => Args4jOption}

/** Arguments for accepting a reference genome. */
trait ReferenceArgs extends DebugLogArgs {
  @Args4jOption(required = false, name = "--reference", usage = "ADAM or FASTA reference genome data")
  var referenceInput: String = ""

  @Args4jOption(required = false, name = "--fragment-length",
    usage = "Sets maximum fragment length. Default value is 10,000. Values greater than 1e9 should be avoided.")
  var fragmentLength: Long = 10000L
}

