package org.hammerlab.guacamole.readsets.args

import org.hammerlab.guacamole.readsets.PerSample
import org.hammerlab.guacamole.readsets.io.Input
import org.hammerlab.guacamole.readsets.rdd.PartitionedRegionsArgs

trait Base extends NoSequenceDictionaryArgs {

  def paths: Array[String]
  def sampleNames: Array[String]

  lazy val inputs: PerSample[Input] = {
    paths.indices.map(i =>
      Input(
        i,
        if (i < sampleNames.length)
          sampleNames(i)
        else
          paths(i),
        paths(i)
      )
    )
  }
}
