package org.hammerlab.guacamole.loci.partitioning

import org.kohsuke.args4j.{Option => Args4JOption}

trait HalfWindowArgs extends HalfWindowConfig {
  @Args4JOption(
    name = "--half-window",
    usage =
      "When partitioning loci and reads, consider reads to overlap loci this far on either side of their ends, in " +
      "order to provide context to offer context to downstream logic."
  )
  private var _halfWindowSize: Int = 0

  override def halfWindowSize: Int = _halfWindowSize
}
