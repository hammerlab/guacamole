package org.hammerlab.guacamole.readsets.io

/**
 * Configuration for read loading. These options should affect performance but not results.
 *
 * @param bamReaderAPI which library to use to load SAM and BAM files
 */
case class ReadLoadingConfig(bamReaderAPI: BamReaderAPI.BamReaderAPI = BamReaderAPI.Best)

object ReadLoadingConfig {
  val default = ReadLoadingConfig()

  /** Given commandline arguments, return a ReadLoadingConfig. */
  def apply(args: ReadLoadingConfigArgs): ReadLoadingConfig = {
    ReadLoadingConfig(
      BamReaderAPI.withNameCaseInsensitive(
        args.bamReaderAPI
      )
    )
  }
}
