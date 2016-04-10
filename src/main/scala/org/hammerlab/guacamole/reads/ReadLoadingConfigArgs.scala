package org.hammerlab.guacamole.reads

import org.hammerlab.guacamole.logging.DebugLogArgs
import org.kohsuke.args4j.{Option => Args4jOption}

/** Arguments for configuring read loading with a ReadLoadingConfig object. */
trait ReadLoadingConfigArgs extends DebugLogArgs {
  @Args4jOption(
    name = "--bam-reader-api",
    usage = "API to use for reading BAMs, one of: best (use samtools if file local), samtools, hadoopbam"
  )
  var bamReaderAPI: String = "best"
}

object ReadLoadingConfigArgs {
  /** Given commandline arguments, return a ReadLoadingConfig. */
  def apply(args: ReadLoadingConfigArgs): ReadLoadingConfig = {
    ReadLoadingConfig(BamReaderAPI.withNameCaseInsensitive(args.bamReaderAPI))
  }
}

