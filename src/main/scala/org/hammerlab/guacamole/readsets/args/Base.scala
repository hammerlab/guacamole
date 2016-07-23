package org.hammerlab.guacamole.readsets.args

import org.hammerlab.guacamole.loci.partitioning.LociPartitionerArgs
import org.hammerlab.guacamole.readsets.PerSample
import org.hammerlab.guacamole.readsets.io.{Input, ReadLoadingConfigArgs}

trait Base
  extends LociPartitionerArgs
    with NoSequenceDictionaryArgs
    with ReadLoadingConfigArgs {

  def paths: Array[String]
  def sampleNames: Array[String]

  lazy val inputs: PerSample[Input] = {
    paths.indices.map(i =>
      Input(
        if (i < sampleNames.length)
          sampleNames(i)
        else
          paths(i),
        paths(i)
      )
    )
  }
}
