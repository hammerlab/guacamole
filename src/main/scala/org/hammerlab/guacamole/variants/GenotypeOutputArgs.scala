package org.hammerlab.guacamole.variants

import org.hammerlab.guacamole.logging.DebugLogArgs
import org.kohsuke.args4j.{Option => Args4jOption}

/** Argument for writing output genotypes. */
trait GenotypeOutputArgs extends DebugLogArgs {
  @Args4jOption(name = "--out", metaVar = "VARIANTS_OUT", required = false,
    usage = "Variant output path. If not specified, print to screen.")
  var variantOutput: String = ""

  @Args4jOption(name = "--out-chunks", metaVar = "X", required = false,
    usage = "When writing out to json format, number of chunks to coalesce the genotypes RDD into.")
  var outChunks: Int = 1

  @Args4jOption(name = "--max-genotypes", metaVar = "X", required = false,
    usage = "Maximum number of genotypes to output. 0 (default) means output all genotypes.")
  var maxGenotypes: Int = 0
}

