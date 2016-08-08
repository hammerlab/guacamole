package org.hammerlab.guacamole.readsets.args

import org.kohsuke.args4j.{Option => Args4jOption}

/** Arguments for accepting two sets of reads (tumor + normal). */
trait TumorNormalReadsArgs extends Base {

  @Args4jOption(name = "--normal-reads", metaVar = "X", required = true, usage = "Aligned reads: normal")
  var normalReads: String = ""

  @Args4jOption(name = "--tumor-reads", metaVar = "X", required = true, usage = "Aligned reads: tumor")
  var tumorReads: String = ""

  override def paths = Array(normalReads, tumorReads)

  override def sampleNames = Array("normal", "tumor")
}
