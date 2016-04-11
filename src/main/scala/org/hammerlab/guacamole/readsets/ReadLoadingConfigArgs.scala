package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.Common.Arguments.Base
import org.hammerlab.guacamole.reads.{BamReaderAPI, ReadLoadingConfig}
import org.kohsuke.args4j.{Option => Args4jOption}

/** Argument for configuring read loading with a ReadLoadingConfig object. */
trait ReadLoadingConfigArgs extends Base {
  @Args4jOption(name = "--bam-reader-api",
    usage = "API to use for reading BAMs, one of: best (use samtools if file local), samtools, hadoopbam")
  var bamReaderAPI: String = "best"
}

object ReadLoadingConfigArgs {
  /** Given commandline arguments, return a ReadLoadingConfig. */
  def fromArguments(args: ReadLoadingConfigArgs): ReadLoadingConfig = {
    ReadLoadingConfig(
      BamReaderAPI.withNameCaseInsensitive(
        args.bamReaderAPI
      )
    )
  }
}

