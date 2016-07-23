package org.hammerlab.guacamole.readsets.args

import org.kohsuke.args4j.{Option => Args4jOption}

/** Argument for accepting a single set of reads (for non-somatic variant calling). */
trait SingleSampleArgs extends Base {
  @Args4jOption(name = "--reads", metaVar = "X", required = true, usage = "Aligned reads")
  var reads: String = ""

  override def paths = Array(reads)

  def sampleName = "reads"

  override def sampleNames = Array(sampleName)
}

