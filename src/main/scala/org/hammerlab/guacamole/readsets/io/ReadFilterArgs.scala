package org.hammerlab.guacamole.readsets.io

import org.apache.hadoop.conf.Configuration
import org.hammerlab.guacamole.loci.args.CallLociArgs
import org.hammerlab.guacamole.loci.parsing.ParsedLoci
import org.kohsuke.args4j.{Option => Args4jOption}

trait ReadFilterArgs extends CallLociArgs {
  @Args4jOption(name = "--min-alignment-quality",
    usage = "Minimum read mapping quality for a read (Phred-scaled)")
  var minAlignmentQuality: Int = 0

  @Args4jOption(name = "--include-duplicates",
    usage = "Include reads marked as duplicates")
  var includeDuplicates: Boolean = false

  @Args4jOption(name = "--include-failed-quality-checks",
    usage = "Include reads that failed vendor quality checks")
  var includeFailedQualityChecks: Boolean = false

  @Args4jOption(name = "--include-single-end",
    usage = "Include single-end reads")
  var includeSingleEnd: Boolean = false

  @Args4jOption(name = "--only-mapped-reads",
    usage = "Include only mapped reads",
    forbids = Array("--loci", "--loci-file")
  )
  var onlyMappedReads: Boolean = false

  @Args4jOption(name = "--split-size", usage = "Maximum HDFS split size")
  var splitSize: Int = -1

  def parseFilters(hadoopConfiguration: Configuration): InputFilters = {
    val loci = ParsedLoci.fromArgs(lociStrOpt, lociFileOpt, hadoopConfiguration)
    InputFilters(
      overlapsLociOpt =
        if (onlyMappedReads)
          Some(ParsedLoci.all)
        else
          loci,
      nonDuplicate = !includeDuplicates,
      passedVendorQualityChecks = !includeFailedQualityChecks,
      isPaired = !includeSingleEnd,
      minAlignmentQuality = minAlignmentQuality,
      maxSplitSizeOpt = if (splitSize > 0) Some(splitSize) else None
    )
  }
}
